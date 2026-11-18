"""Ретропроверка правил гололёда и снегопада на исходах — ДТП Новосибирска (исследование погоды 17.11.2026).

Исход — ДТП в г. Новосибирске с отметкой «Недостатки зимнего содержания» в поле road_conditions
(открытые данные ДТП-Стат по Новосибирской области, https://dtp-stat.ru/opendata — условие
использования: активная ссылка на сайт). Погода — суточные значения Open-Meteo по точке города.
Сравнивается частота таких ДТП в дни срабатывания правила и в остальные дни — в целом за
ноябрь–март и внутри каждого месяца (сезонность не должна выдавать себя за правило).

Правила:
- «переход через 0» — минимум суток ниже 0 °C, максимум выше (прежнее правило табло);
- «дождь и ночной мороз» — днём дождь от 1 мм, следующей ночью минимум до −1 °C; исход — на
  следующие сутки (правило «возможна гололедица», ecology_rules 1.1);
- «снег от 2 см» и «снег от 5 см» за сутки.

Только стандартная библиотека:

    python3 -I scripts/ice_retro/ice_dtp_check.py <novosibirskaia-oblast.geojson> <погода.json>

Погоду брать с явной моделью: ...archive?models=era5&daily=temperature_2m_min,temperature_2m_max,
snowfall_sum,precipitation_sum,rain_sum — без models архив для 2017+ отдаёт ECMWF IFS (урок
open-meteo-archive-default-is-forecast-not-reanalysis).
"""
import collections
import json
import sys

MONTHS = (11, 12, 1, 2, 3)


def main(dtp_path: str, wx_path: str) -> None:
    with open(dtp_path, encoding="utf-8") as f:
        feats = json.load(f)["features"]
    wd = collections.Counter()
    for x in feats:
        p = x["properties"]
        if (p.get("address") or "").startswith("г Новосибирск") and \
                "Недостатки зимнего содержания" in (p.get("road_conditions") or []):
            wd[p["datetime"][:10]] += 1
    with open(wx_path, encoding="utf-8") as f:
        w = json.load(f)
    e = w["daily"]
    days = [dict(t=t, mn=mn, mx=mx, sn=sn, rain=rn) for t, mn, mx, sn, rn in zip(
        e["time"], e["temperature_2m_min"], e["temperature_2m_max"], e["snowfall_sum"], e.get("rain_sum") or [None] * len(e["time"]))]
    print(f"Погода: точка {w.get('latitude')}, {w.get('longitude')}; суток {len(days)}, {days[0]['t']} … {days[-1]['t']}")
    print(f"ДТП «недостатки зимнего содержания» в г. Новосибирске: {sum(wd.values())}, дней с ними {len(wd)}; "
          f"{min(wd)} … {max(wd)}\n")

    # (сутки исхода, сработало ли правило) — для правила с исходом «на следующие сутки» сдвиг
    def cases(rule: str):
        out = []
        for i, d in enumerate(days):
            if int(d["t"][5:7]) not in MONTHS or d["mn"] is None:
                continue
            if rule == "переход через 0":
                out.append((d["t"], d["mn"] < 0 < d["mx"]))
            elif rule == "снег от 2 см":
                out.append((d["t"], (d["sn"] or 0) >= 2.0))
            elif rule == "снег от 5 см":
                out.append((d["t"], (d["sn"] or 0) >= 5.0))
            elif rule == "дождь и ночной мороз":
                if i == 0 or days[i - 1]["rain"] is None:
                    continue
                prev = days[i - 1]
                out.append((d["t"], (prev["rain"] or 0) >= 1.0 and d["mn"] <= -1.0))
        return out

    for rule in ("переход через 0", "дождь и ночной мороз", "снег от 2 см", "снег от 5 см"):
        cs = cases(rule)
        on = [wd.get(t, 0) for t, r in cs if r]
        off = [wd.get(t, 0) for t, r in cs if not r]
        a = sum(1 for t, r in cs if r and wd.get(t, 0) >= 1)
        b = sum(1 for t, r in cs if r and wd.get(t, 0) == 0)
        c = sum(1 for t, r in cs if not r and wd.get(t, 0) >= 1)
        pod = a / (a + c) if a + c else float("nan")
        far = b / (a + b) if a + b else float("nan")
        m_on = sum(on) / len(on) if on else float("nan")
        m_off = sum(off) / len(off) if off else float("nan")
        print(f"═══ {rule} (ноябрь–март) ═══")
        print(f"  дней правила {len(on)}, ДТП в сутки: в дни правила {m_on:.2f}, в остальные {m_off:.2f}, "
              f"отношение {m_on / m_off:.2f}; POD {pod:.2f}, FAR {far:.2f} (исход — хотя бы одно ДТП)")
        parts = []
        for m in MONTHS:
            on_m = [wd.get(t, 0) for t, r in cs if r and int(t[5:7]) == m]
            off_m = [wd.get(t, 0) for t, r in cs if not r and int(t[5:7]) == m]
            if on_m and off_m:
                parts.append(f"{m:02d}: {sum(on_m) / len(on_m):.2f}/{sum(off_m) / len(off_m):.2f} ({len(on_m)} дн.)")
            else:
                parts.append(f"{m:02d}: —")
        print("  по месяцам (в дни правила / в остальные): " + "; ".join(parts) + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
