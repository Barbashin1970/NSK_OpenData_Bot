"""Сборка аналитической таблицы по отключениям ЖКХ Новосибирска из бэкапов СИМА.

Набор 2026-11-16, собранный этим скриптом, лежит архивом с контрольными суммами в
docs/nir-sima-2026/artefakty/ (опись — README.md там же). Бэкапы в репозиторий не входят.

Запуск (только стандартная библиотека, Python 3.10+):

    python3 -I build_dataset.py <папка_вывода> <бэкап_самый_свежий> [<бэкап_старше> ...]

    python3 -I scripts/zhkh_calibration/build_dataset.py \
        ~/Downloads/sima-zhkh-dataset-2026-11-16 \
        ~/Downloads/sigma-backup-all-2026-11-16-1254 \
        ~/Downloads/sigma-backup-all-2026-08-19-1234 \
        ~/Downloads/sigma-backup-all-2026-03-31-1153

Из самого свежего бэкапа берутся суточный архив (power_daily_archive) и погода
(ecology_daily_archive). Из каждого бэкапа берётся его окно сырых снимков
(power_outages): они дают точное число снимков за сутки, почасовую детализацию
и сутки, которых нет в архиве (25–26 марта).
"""

import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

if len(sys.argv) < 3:
    sys.exit(__doc__)
OUT = Path(sys.argv[1]).expanduser()
BACKUPS = [Path(p).expanduser() for p in sys.argv[2:]]
OUT.mkdir(parents=True, exist_ok=True)

UTILITIES = {  # название на сайте 051 → код
    "Электроснабжение": "electricity",
    "Теплоснабжение": "heating",
    "Горячее водоснабжение": "hot_water",
    "Холодное водоснабжение и канализация": "cold_water_sewer",
    "Газоснабжение": "gas",
}
DISTRICTS = [
    "Дзержинский район", "Железнодорожный район", "Заельцовский район",
    "Калининский район", "Кировский район", "Ленинский район",
    "Октябрьский район", "Первомайский район", "Советский район",
    "Центральный район",
]
# group_type в СИМА → раздел на 051.novo-sibirsk.ru/sitepages/off.aspx
KIND = {"active": "emergency",   # «Аварийные отключения жилого фонда» — «Отключено домов»
        "planned": "planned"}    # «Плановые отключения жилого фонда» — «Запланировано к отключению»
# Нерабочие праздничные дни в диапазоне данных — по ТК РФ ст. 112
# (9 мая 2026 — суббота, выходной переносится на понедельник 11 мая).
HOLIDAYS = {"2026-05-01", "2026-05-09", "2026-05-11", "2026-06-12", "2026-11-04"}
NSK_TZ = timezone(timedelta(hours=7))  # Новосибирск, без перехода на летнее время
LOW_SAMPLING = 20                      # меньше 20 снимков за сутки — сбор был неполным
METRICS = ("houses_sum", "records")


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def empty() -> dict:
    return {"emergency_houses_sum": 0, "planned_houses_sum": 0,
            "emergency_records": 0, "planned_records": 0, "snapshots_row": 0}


# ── суточный архив из самого свежего бэкапа ────────────────────────────────
arch = load(BACKUPS[0] / "power_daily_archive.json")
eco = load(BACKUPS[0] / "ecology_daily_archive.json")

agg: dict[tuple, dict] = {}
power_max: dict[str, int] = defaultdict(int)   # включая строки district='all' за 27–28.03
for r in arch:
    power_max[r["day"]] = max(power_max[r["day"]], r["snapshots"])
    if r["district"] == "all":
        continue
    agg[(r["day"], r["district"], r["utility"])] = {
        "emergency_houses_sum": r["active_houses"], "planned_houses_sum": r["planned_houses"],
        "emergency_records": r["active_records"], "planned_records": r["planned_records"],
        "snapshots_row": r["snapshots"],
    }
archive_days = set(power_max)

# ── окна сырых снимков из всех бэкапов ─────────────────────────────────────
# Истинное число снимков за сутки. Первый день окна обрезан хранением (30 дней),
# последний день окна старого бэкапа неполон (момент выгрузки), а в архиве он
# полный — такие дни из «истины» выбрасываются. Если дня нет в архиве вовсе,
# окно — единственный источник, и сутки пересчитываются из сырых снимков
# по той же формуле, что и архив (сверено: 639 из 639 строк совпали).
truth: dict[str, int] = {}
snap_rows = []
extra_snaps: dict[tuple, set] = defaultdict(set)
for i, b in enumerate(BACKUPS):
    tag = load(b / "manifest.json")["created_at"][:16]
    raw = load(b / "power_outages.json")
    per_day = Counter(ts[:10] for ts in {r["scraped_at"] for r in raw})
    days = sorted(per_day)
    for j, d in enumerate(days):
        if d not in archive_days:
            truth[d] = per_day[d]
        elif j > 0 and (i == 0 or j < len(days) - 1):
            truth[d] = per_day[d]
    for r in raw:
        ts = datetime.fromisoformat(r["scraped_at"])
        loc = ts.astimezone(NSK_TZ)
        snap_rows.append({
            "scraped_at_utc": ts.astimezone(timezone.utc).isoformat(timespec="seconds"),
            "scraped_at_nsk": loc.isoformat(timespec="seconds"),
            "day_nsk": loc.date().isoformat(),
            "hour_nsk": loc.hour,
            "utility_code": UTILITIES.get(r["utility"], r["utility"]),
            "utility": r["utility"],
            "kind": KIND.get(r["group_type"], r["group_type"]),
            "district": r["district"],
            "houses": r["houses"],
            "source_backup": tag,
        })
        d = r["scraped_at"][:10]
        if d in archive_days or r["district"] == "all":
            continue
        k = (d, r["district"], r["utility"])
        x = agg.setdefault(k, empty())
        kind = KIND[r["group_type"]]
        x[f"{kind}_houses_sum"] += r["houses"]
        x[f"{kind}_records"] += 1
        extra_snaps[k].add(r["scraped_at"])
for k, s in extra_snaps.items():
    agg[k]["snapshots_row"] = len(s)

collected_days = archive_days | {d for d in truth if d not in archive_days}
day_first, day_last = min(collected_days), max(collected_days)

eco_max: dict[str, int] = defaultdict(int)
for r in eco:
    eco_max[r["day"]] = max(eco_max[r["day"]], r["snapshots"] or 0)

# ── температура ────────────────────────────────────────────────────────────
temp = {(r["day"], r["district"]): r["temp_avg"] for r in eco if r["temp_avg"] is not None}
temp_city = {}
for d in {k[0] for k in temp}:
    vals = [temp[(d, dist)] for dist in DISTRICTS if (d, dist) in temp]
    if vals:
        temp_city[d] = round(statistics.median(vals), 1)

# ── календарь ──────────────────────────────────────────────────────────────
d0, d1 = date.fromisoformat(day_first), date.fromisoformat(day_last)
calendar = [(d0 + timedelta(i)).isoformat() for i in range((d1 - d0).days + 1)]


def cal_fields(d: str) -> dict:
    dt = date.fromisoformat(d)
    return {"day": d, "month": d[:7], "iso_week": dt.isocalendar()[1],
            "dow": dt.isoweekday(), "is_weekend": int(dt.isoweekday() >= 6),
            "is_holiday": int(d in HOLIDAYS)}


def day_status(d: str) -> dict:
    if d not in collected_days:
        return {"collected": 0, "partial_day": 0, "low_sampling": 0,
                "n_snapshots_day": "", "n_snapshots_source": "none"}
    if d in truth:
        n, src = truth[d], "raw"
    else:
        n, src = max(power_max[d], eco_max.get(d, 0)), "estimate"
    return {"collected": 1, "partial_day": int(d == day_last),
            "low_sampling": int(n < LOW_SAMPLING),
            "n_snapshots_day": n, "n_snapshots_source": src}


VALUE_KEYS = ("emergency_houses_avg", "planned_houses_avg",
              "emergency_houses_listed", "planned_houses_listed",
              "emergency_time_share", "planned_time_share",
              "emergency_any", "planned_any",
              "emergency_houses_sum", "planned_houses_sum",
              "emergency_records", "planned_records", "snapshots_row")

# ── таблица 1: день × район × услуга ───────────────────────────────────────
rows_district = []
city = defaultdict(lambda: defaultdict(float))
for d in calendar:
    st = day_status(d)
    n = st["n_snapshots_day"]
    for dist in DISTRICTS:
        for util, code in UTILITIES.items():
            row = cal_fields(d) | {"district": dist, "utility_code": code, "utility": util} | st
            if st["collected"]:
                x = agg.get((d, dist, util), empty())
                sr = x["snapshots_row"]
                vals = {
                    "emergency_houses_avg": round(x["emergency_houses_sum"] / n, 2),
                    "planned_houses_avg": round(x["planned_houses_sum"] / n, 2),
                    "emergency_houses_listed": round(x["emergency_houses_sum"] / x["emergency_records"], 2) if x["emergency_records"] else 0,
                    "planned_houses_listed": round(x["planned_houses_sum"] / x["planned_records"], 2) if x["planned_records"] else 0,
                    "emergency_time_share": round(min(x["emergency_records"] / n, 1.0), 3),
                    "planned_time_share": round(min(x["planned_records"] / n, 1.0), 3),
                    "emergency_any": int(x["emergency_records"] > 0),
                    "planned_any": int(x["planned_records"] > 0),
                } | x
                c = city[(d, code)]
                c["emergency_houses_avg"] += x["emergency_houses_sum"] / n
                c["planned_houses_avg"] += x["planned_houses_sum"] / n
                c["emergency_houses_listed"] += x["emergency_houses_sum"] / x["emergency_records"] if x["emergency_records"] else 0
                c["planned_houses_listed"] += x["planned_houses_sum"] / x["planned_records"] if x["planned_records"] else 0
                c["districts_with_emergency"] += vals["emergency_any"]
                c["districts_with_planned"] += vals["planned_any"]
            else:
                vals = {k: "" for k in VALUE_KEYS}
            row |= vals | {"temp_avg_district": temp.get((d, dist), ""),
                           "temp_avg_city": temp_city.get(d, "")}
            rows_district.append(row)

# ── таблица 2: день × услуга по городу ─────────────────────────────────────
rows_city = []
CITY_KEYS = ("emergency_houses_avg", "planned_houses_avg", "emergency_houses_listed",
             "planned_houses_listed", "districts_with_emergency", "districts_with_planned")
for d in calendar:
    st = day_status(d)
    for util, code in UTILITIES.items():
        c = city.get((d, code), {})
        vals = {k: (round(c.get(k, 0), 2) if "houses" in k else int(c.get(k, 0)))
                if st["collected"] else "" for k in CITY_KEYS}
        rows_city.append(cal_fields(d) | {"utility_code": code, "utility": util} | st | vals
                         | {"temp_avg_city": temp_city.get(d, "")})

snap_rows.sort(key=lambda x: (x["scraped_at_utc"], x["utility_code"], x["kind"], x["district"]))


def write(name: str, rows: list[dict]) -> None:
    with open(OUT / name, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"{name}: {len(rows)} строк, {len(rows[0])} колонок")


write("zhkh_daily_district.csv", rows_district)
write("zhkh_daily_city.csv", rows_city)
write("zhkh_snapshots.csv", snap_rows)

# ── сторож: плитки «Отчёта за всё время» СИМА воспроизводятся из таблицы ──
# В отчёте: по дням сумма *_listed по всем районам и услугам, округление до целого.
ui = defaultdict(lambda: [0.0, 0.0])
for r in rows_city:
    if r["collected"] and r["day"] in archive_days:
        ui[r["day"]][0] += r["emergency_houses_listed"]
        ui[r["day"]][1] += r["planned_houses_listed"]
E = {d: round(v[0]) for d, v in ui.items()}
P = {d: round(v[1]) for d, v in ui.items()}
with_e = [d for d in E if E[d] > 0]
print("сверка с отчётом СИМА (дни из архива):",
      f"дней {len(E)}, чистых {sum(1 for d in E if E[d] == 0 and P[d] == 0)},",
      f"с авариями {len(with_e)}, макс {max(E.values())} ({max(E, key=E.get)}),",
      f"среднее {round(sum(E.values()) / len(with_e))}, плановых сумма {sum(P.values())}")

src = Counter(day_status(d)["n_snapshots_source"] for d in calendar)
print(f"календарь {day_first}..{day_last}: {len(calendar)} суток, со сбором {len(collected_days)},",
      f"пропусков {len(calendar) - len(collected_days)}; число снимков за сутки: {dict(src)}")
print("неполный сбор (<20 снимков):", sorted(d for d in calendar if day_status(d)["low_sampling"]))
