"""Синтетический индекс дорожной нагрузки Новосибирска.

Оценка 0–10 на основе:
  • Времени суток (паттерны часов пик НСК)
  • Дня недели + понедельничный эффект
  • Официальных праздников и предпраздничных дней (2025–2027)
  • Текущих погодных условий (Open-Meteo, из кэша)
  • Городских событий (1 сентября, дни выборов, вечер накануне каникул)

Реальных данных о пробках нет — только аналитическая модель.
Новосибирский часовой пояс: UTC+7.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .city_config import get_utc_offset
from .rule_engine import rules as _rules

log = logging.getLogger(__name__)

_NSK_UTC_OFFSET = get_utc_offset()


def _build_holiday_db() -> dict[date, str]:
    """Строит словарь нерабочих дней из holiday_calendar.yaml."""
    cal = _rules.get("holiday_calendar")
    db: dict[date, str] = {}
    for entry in cal.get("holidays", []):
        try:
            d = date.fromisoformat(entry["date"])
            db[d] = entry["type"]
        except Exception as exc:
            log.warning("holiday_calendar: некорректная запись %s: %s", entry, exc)
    return db


# ── База официальных нерабочих дней 2025–2027 ────────────────────────────────
# Загружается из config/rules/holiday_calendar.yaml
# Тип записи:
#   "holiday"      — нерабочий праздничный день (= выходной)
#   "bridge"       — перенесённый выходной (мост между праздником и уик-эндом)
#   "post_holiday" — первый рабочий день после длинных каникул (повышенный трафик)
#   "pre"          — предпраздничный укороченный рабочий день

_HOLIDAY_DB: dict[date, str] = {

    # ── 2025 ────────────────────────────────────────────────────────────────
    # Новогодние каникулы + Рождество: 1–8 января
    date(2025,  1,  1): "holiday",
    date(2025,  1,  2): "holiday",
    date(2025,  1,  3): "holiday",
    date(2025,  1,  4): "holiday",
    date(2025,  1,  5): "holiday",
    date(2025,  1,  6): "holiday",
    date(2025,  1,  7): "holiday",
    date(2025,  1,  8): "holiday",
    date(2025,  1,  9): "post_holiday",  # возврат всех в город
    # 23 февраля (вс) → перенос на пн 24; пятница 22-го — предпраздничный
    date(2025,  2, 22): "pre",
    date(2025,  2, 24): "holiday",
    # 8 марта (сб) → перенос на пн 10; пятница 7-го — предпраздничный
    date(2025,  3,  7): "pre",
    date(2025,  3, 10): "holiday",
    # Майские: 1 мая (чт), 2 (пт-мост), 8 (чт-мост), 9 мая (пт)
    date(2025,  4, 30): "pre",
    date(2025,  5,  1): "holiday",
    date(2025,  5,  2): "bridge",
    date(2025,  5,  8): "bridge",
    date(2025,  5,  9): "holiday",
    # 12 июня (чт) + пт-мост; среда 11-го — предпраздничный
    date(2025,  6, 11): "pre",
    date(2025,  6, 12): "holiday",
    date(2025,  6, 13): "bridge",
    # 4 ноября (вт) + пн-мост
    date(2025, 11,  3): "bridge",
    date(2025, 11,  4): "holiday",
    # 31 декабря — укороченный рабочий день
    date(2025, 12, 31): "pre",

    # ── 2026 ────────────────────────────────────────────────────────────────
    date(2026,  1,  1): "holiday",
    date(2026,  1,  2): "holiday",
    date(2026,  1,  3): "holiday",
    date(2026,  1,  4): "holiday",
    date(2026,  1,  5): "holiday",
    date(2026,  1,  6): "holiday",
    date(2026,  1,  7): "holiday",
    date(2026,  1,  8): "holiday",
    date(2026,  1,  9): "post_holiday",
    # 23 февраля (пн) — само по себе
    date(2026,  2, 23): "holiday",
    # 8 марта (вс) → пн 9-го; пятница 6-го — предпраздничный
    date(2026,  3,  6): "pre",
    date(2026,  3,  9): "holiday",
    # 1 мая (пт); чт 30 апр — предпраздничный
    date(2026,  4, 30): "pre",
    date(2026,  5,  1): "holiday",
    # 9 мая (сб) → пт 8-го выходной (перенос по декрету)
    date(2026,  5,  8): "holiday",
    # 12 июня (пт) — само по себе удобно; чт 11-го — предпраздничный
    date(2026,  6, 11): "pre",
    date(2026,  6, 12): "holiday",
    # 4 ноября (ср); вт 3-го — предпраздничный
    date(2026, 11,  3): "pre",
    date(2026, 11,  4): "holiday",
    date(2026, 12, 31): "pre",

    # ── 2027 ────────────────────────────────────────────────────────────────
    date(2027,  1,  1): "holiday",
    date(2027,  1,  2): "holiday",
    date(2027,  1,  3): "holiday",
    date(2027,  1,  4): "holiday",
    date(2027,  1,  5): "holiday",
    date(2027,  1,  6): "holiday",
    date(2027,  1,  7): "holiday",
    date(2027,  1,  8): "holiday",
    date(2027,  1,  9): "post_holiday",
    # 23 февраля (вт); пн 22-го — предпраздничный
    date(2027,  2, 22): "pre",
    date(2027,  2, 23): "holiday",
    # 8 марта (пн) — отлично; пт 5-го — предпраздничный
    date(2027,  3,  5): "pre",
    date(2027,  3,  8): "holiday",
    # 1 мая (сб) → пн 3-го; пт 30 апр — предпраздничный
    date(2027,  4, 30): "pre",
    date(2027,  5,  3): "holiday",
    # 9 мая (вс) → пн 10-го; пт 7-го — предпраздничный
    date(2027,  5,  7): "pre",
    date(2027,  5, 10): "holiday",
    # 12 июня (сб) → пн 14-го; пт 11-го — предпраздничный
    date(2027,  6, 11): "pre",
    date(2027,  6, 14): "holiday",
    # 4 ноября (чт) + пт-мост; ср 3-го — предпраздничный
    date(2027, 11,  3): "pre",
    date(2027, 11,  4): "holiday",
    date(2027, 11,  5): "bridge",
    date(2027, 12, 31): "pre",
}

# Дополнительные городские события — загружаются из traffic_rules.yaml
# Формат: (month, day, label, traffic_delta, morning_only, emoji)
def _build_annual_events() -> list[tuple[int, int, str, float, bool, str]]:
    evs = _rules.get("traffic_rules").get("annual_events", [])
    return [
        (e["month"], e["day"], e.get("label", ""), float(e.get("delta", 0)),
         bool(e.get("morning_only", False)), e.get("emoji", "📅"))
        for e in evs
    ]


def _build_wmo_codes() -> tuple[set, set, set, set, set]:
    codes = _rules.get("traffic_rules").get("weather", {}).get("wmo_codes", {})
    return (
        set(codes.get("snow",  [71, 73, 75, 77, 85, 86])),
        set(codes.get("rain",  [51, 53, 55, 61, 63, 65, 80, 81, 82])),
        set(codes.get("ice",   [56, 57, 66, 67])),
        set(codes.get("storm", [95, 96, 99])),
        set(codes.get("fog",   [45, 48])),
    )


_ANNUAL_EVENTS = _build_annual_events()

# WMO weathercode → категория осадков (из traffic_rules.yaml)
_SNOW_CODES, _RAIN_CODES, _ICE_CODES, _STORM_CODES, _FOG_CODES = _build_wmo_codes()


@dataclass
class TrafficFactor:
    name: str
    description: str
    delta: float
    emoji: str = ""


@dataclass
class TrafficIndex:
    index: float                            # 0.0–10.0
    level: str                              # "Свободно" … "Коллапс"
    emoji: str
    citizen_tip: str = ""
    official_tip: str = ""
    timestamp: str = ""
    next_peak: str = ""
    factors: list[TrafficFactor] = field(default_factory=list)


# ── Паттерны часов пик (из traffic_rules.yaml) ───────────────────────────────

# (start_hour, end_hour, base_score, label)
def _build_time_zones() -> list[tuple[float, float, float, str]]:
    zones = _rules.get("traffic_rules").get("time_zones", [])
    if not zones:
        # fallback: оригинальные значения
        return [
            ( 0.0,  6.0, 0.3,  "Ночь"),
            ( 6.0,  7.5, 1.8,  "Раннее утро"),
            ( 7.5,  9.5, 5.5,  "Утренний час пик"),
            ( 9.5, 11.0, 3.2,  "Постпиковое утро"),
            (11.0, 13.0, 2.8,  "Середина дня"),
            (13.0, 14.5, 3.5,  "Обеденное время"),
            (14.5, 16.5, 3.0,  "Послеобеденное время"),
            (16.5, 19.0, 5.0,  "Вечерний час пик"),
            (19.0, 22.0, 2.5,  "Вечер"),
            (22.0, 24.0, 1.2,  "Поздний вечер"),
        ]
    return [(z["start"], z["end"], z["score"], z.get("label", "")) for z in zones]


_TIME_ZONES = _build_time_zones()


def _nsk_now() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=_NSK_UTC_OFFSET)


def _time_base(hour: float) -> tuple[float, str]:
    for start, end, base, label in _TIME_ZONES:
        if start <= hour < end:
            return base, label
    return 0.3, "Ночь"


def _day_factors(
    dow: int,
    hour: float,
    today: date,
    is_holiday: bool,
    is_pre: bool,
    day_type: str,
) -> tuple[float, list[TrafficFactor]]:
    """Модификаторы по дню недели и специальным событиям (из traffic_rules.yaml)."""
    factors: list[TrafficFactor] = []
    mod = 0.0
    dm = _rules.get("traffic_rules").get("day_modifiers", {})

    # Понедельник: все сонные едут на работу → сильнее по утрам
    mon = dm.get("monday", {})
    if dow == 0 and not is_holiday and mon.get("hours_from", 7.0) <= hour < mon.get("hours_to", 10.0):
        delta = float(mon.get("delta", 0.8))
        mod += delta
        factors.append(TrafficFactor(
            mon.get("name", "Понедельничный эффект"),
            mon.get("description", "Все возвращаются к работе после выходных — движение плотнее"),
            delta, mon.get("emoji", "😴"),
        ))

    # Пятница: вечерний исход усилен
    fri = dm.get("friday", {})
    if dow == 4 and not is_holiday and hour >= fri.get("hours_from", 16.0):
        delta = float(fri.get("delta", 0.7))
        mod += delta
        factors.append(TrafficFactor(
            fri.get("name", "Пятничный исход"),
            fri.get("description", "Все спешат покинуть город на выходные — вечерний пик усилен"),
            delta, fri.get("emoji", "🏁"),
        ))

    # Предпраздничный день: часть людей уезжает раньше → лёгкое снижение
    pre = dm.get("pre_holiday", {})
    if is_pre and not is_holiday:
        delta = float(pre.get("delta", -0.4))
        mod += delta
        factors.append(TrafficFactor(
            pre.get("name", "Предпраздничный день"),
            pre.get("description", "Укороченный рабочий день — многие уезжают раньше обычного"),
            delta, pre.get("emoji", "🎉"),
        ))

    # Пост-праздник: возврат из каникул (Jan 9 и аналоги)
    post = dm.get("post_holiday", {})
    if day_type == "post_holiday":
        delta = float(post.get("delta", 1.2))
        mod += delta
        factors.append(TrafficFactor(
            post.get("name", "Возврат после каникул"),
            post.get("description", "Все возвращаются в город после длинных праздников — пробки выше нормы"),
            delta, post.get("emoji", "🏠➡🏙"),
        ))

    # Ежегодные городские события
    for ev_month, ev_day, ev_label, ev_delta, ev_morning_only, ev_emoji in _ANNUAL_EVENTS:
        if today.month == ev_month and today.day == ev_day:
            if ev_morning_only and not (7.0 <= hour < 10.0):
                continue
            mod += ev_delta
            factors.append(TrafficFactor(ev_label, f"Городское событие: {ev_label}", ev_delta, ev_emoji))

    return mod, factors


def _weather_factors(
    weather: dict | None,
    today: date | None = None,
) -> tuple[float, list[TrafficFactor]]:
    """Вклад погоды в индекс пробок."""
    if not weather:
        return 0.0, []

    factors: list[TrafficFactor] = []
    delta = 0.0

    wcode  = int(weather.get("weathercode") or 0)
    precip = float(weather.get("precipitation") or 0.0)
    temp   = float(weather.get("temperature_c") or 10.0)
    wind   = float(weather.get("wind_speed_ms") or 0.0)

    if today is None:
        today = _nsk_now().date()

    w_cfg = _rules.get("traffic_rules").get("weather", {})

    # ── Снег ─────────────────────────────────────────────────────────────────
    if wcode in _SNOW_CODES:
        snow_cfg = w_cfg.get("snow", {})
        heavy_thr   = float(snow_cfg.get("heavy_threshold_mm", 5.0))
        heavy_d     = float(snow_cfg.get("heavy_delta", 2.5))
        normal_d    = float(snow_cfg.get("normal_delta", 1.5))
        first_d     = float(snow_cfg.get("first_snow_extra_delta", 1.5))
        first_months= set(snow_cfg.get("first_snow_months", [10, 11]))

        if precip > heavy_thr:
            delta += heavy_d
            factors.append(TrafficFactor(
                "Сильный снегопад",
                f"Осадки {precip:.1f} мм — видимость снижена, дороги скользкие",
                heavy_d, "❄️",
            ))
        else:
            delta += normal_d
            factors.append(TrafficFactor(
                "Снегопад", "Снег на дорогах — движение замедляется", normal_d, "🌨",
            ))

        # Первый осенний снег: город не готов
        if today.month in first_months:
            delta += first_d
            factors.append(TrafficFactor(
                "Первый осенний снег",
                "Летняя резина, неубранный снег, неготовность служб — экстремальный риск",
                first_d, "🚨",
            ))

    # ── Дождь ────────────────────────────────────────────────────────────────
    elif wcode in _RAIN_CODES:
        rain_cfg = w_cfg.get("rain", {})
        heavy_thr = float(rain_cfg.get("heavy_threshold_mm", 10.0))
        heavy_d   = float(rain_cfg.get("heavy_delta", 1.5))
        normal_d  = float(rain_cfg.get("normal_delta", 0.8))

        if precip > heavy_thr:
            delta += heavy_d
            factors.append(TrafficFactor(
                "Ливень", f"Интенсивные осадки {precip:.1f} мм — видимость и сцепление снижены", heavy_d, "⛈",
            ))
        else:
            delta += normal_d
            factors.append(TrafficFactor(
                "Дождь", "Мокрое покрытие — водители снижают скорость", normal_d, "🌧",
            ))

    # ── Ледяной дождь / изморозь ──────────────────────────────────────────────
    elif wcode in _ICE_CODES:
        ice_d = float(w_cfg.get("ice", {}).get("delta", 2.5))
        delta += ice_d
        factors.append(TrafficFactor(
            "Ледяной дождь", "Гололедица на дорогах — крайне опасно", ice_d, "🧊",
        ))

    # ── Гроза ────────────────────────────────────────────────────────────────
    elif wcode in _STORM_CODES:
        storm_d = float(w_cfg.get("storm", {}).get("delta", 2.0))
        delta += storm_d
        factors.append(TrafficFactor(
            "Гроза", "Непогода резко снижает видимость и скорость потока", storm_d, "⛈",
        ))

    # ── Туман ────────────────────────────────────────────────────────────────
    elif wcode in _FOG_CODES:
        fog_d = float(w_cfg.get("fog", {}).get("delta", 0.8))
        delta += fog_d
        factors.append(TrafficFactor(
            "Туман", "Видимость снижена — водители едут осторожнее", fog_d, "🌫",
        ))

    # ── Гололедица (температура ≈0 + осадки) ────────────────────────────────
    bi_cfg = w_cfg.get("black_ice", {})
    bi_min = float(bi_cfg.get("temp_min", -3.0))
    bi_max = float(bi_cfg.get("temp_max", 2.0))
    bi_d   = float(bi_cfg.get("delta", 1.0))
    if bi_min <= temp <= bi_max and precip > 0 and wcode not in _ICE_CODES:
        delta += bi_d
        factors.append(TrafficFactor(
            "Риск гололедицы",
            f"Температура {temp:.0f}°C + осадки — возможна гололедица на мостах и тенистых участках",
            bi_d, "🧊",
        ))

    # ── Экстремальный мороз: машины не заводятся, меньше автомобилей ─────────
    fr_cfg    = w_cfg.get("frost", {})
    fr_ext_t  = float(fr_cfg.get("extreme_threshold", -30.0))
    fr_ext_d  = float(fr_cfg.get("extreme_delta", -1.5))
    fr_str_t  = float(fr_cfg.get("strong_threshold", -20.0))
    fr_str_d  = float(fr_cfg.get("strong_delta", -0.5))

    if temp < fr_ext_t:
        delta += fr_ext_d
        factors.append(TrafficFactor(
            "Экстремальный мороз",
            f"{temp:.0f}°C — массовый отказ техники, горожане пересаживаются на ОТ",
            fr_ext_d, "🥶",
        ))
    elif temp < fr_str_t:
        delta += fr_str_d
        factors.append(TrafficFactor(
            "Сильный мороз",
            f"{temp:.0f}°C — часть авто не заводится, трафик ниже среднего",
            fr_str_d, "🥶",
        ))

    # ── Сильный ветер ────────────────────────────────────────────────────────
    wind_cfg = w_cfg.get("wind", {})
    wind_thr = float(wind_cfg.get("strong_threshold_ms", 15.0))
    wind_d   = float(wind_cfg.get("strong_delta", 0.5))
    if wind > wind_thr:
        delta += wind_d
        factors.append(TrafficFactor(
            "Сильный ветер", f"{wind:.0f} м/с — снижение скорости на открытых участках", wind_d, "💨",
        ))

    return delta, factors


def _classify(index: float) -> tuple[str, str]:
    levels = _rules.get("traffic_rules").get("levels", [])
    for lvl in levels:
        if index < float(lvl["max"]):
            return lvl["label"], lvl.get("emoji", "")
    # fallback
    if index < 2.0:   return "Свободно",     "🟢"
    if index < 3.5:   return "Умеренно",     "🟡"
    if index < 5.0:   return "Затруднено",   "🟠"
    if index < 6.5:   return "Сложно",       "🔴"
    if index < 8.5:   return "Очень сложно", "🔴"
    return "Коллапс", "⛔"


def _next_peak_desc(hour: float, dow: int, is_holiday: bool) -> str:
    is_rest = dow >= 5 or is_holiday
    if is_rest:
        return "Выходной / праздник — часов пик нет"
    if hour < 7.5:
        return "Сегодня в 07:30 — утренний час пик"
    elif hour < 9.5:
        return "Сейчас утренний час пик (до 09:30)"
    elif hour < 16.5:
        return "Сегодня в 16:30 — вечерний час пик"
    elif hour < 19.0:
        return "Сейчас вечерний час пик (до 19:00)"
    else:
        return "Следующий час пик: завтра в 07:30"


def _build_tips(index: float, factors: list[TrafficFactor]) -> tuple[str, str]:
    fnames = {f.name for f in factors}

    # Определяем ключ уровня по порогам из YAML
    levels = _rules.get("traffic_rules").get("levels", [])
    level_keys = ["free", "moderate", "difficult", "complex", "very_complex", "collapse"]
    level_key = "collapse"
    for i, lvl in enumerate(levels):
        if index < float(lvl["max"]) and i < len(level_keys):
            level_key = level_keys[i]
            break

    today = _nsk_now().date()
    citizen  = _rules.tip(level_key, "citizen",  today)
    official = _rules.tip(level_key, "official", today)

    # Специфические уточнения по активным факторам
    fh = _rules.get("traffic_rules").get("factor_hints", {})
    dm = _rules.get("traffic_rules").get("day_modifiers", {})
    post_name = dm.get("post_holiday", {}).get("name", "Возврат после каникул")
    mon_name  = dm.get("monday",       {}).get("name", "Понедельничный эффект")
    fri_name  = dm.get("friday",       {}).get("name", "Пятничный исход")

    if "Первый осенний снег" in fnames:
        citizen += " " + fh.get("first_snow", "⚠️ Первый снег — экстремально скользко! Пересядьте на метро.")
    if "Ледяной дождь" in fnames or "Риск гололедицы" in fnames:
        citizen += " " + fh.get("ice", "🧊 Гололедица: при поездке снизьте скорость вдвое.")
    if mon_name in fnames:
        citizen += " " + fh.get("monday", "Выезжайте до 07:30 или после 09:30.")
    if fri_name in fnames:
        citizen += " " + fh.get("friday", "Пятничный вечер: выезжайте после 20:00 или используйте ОТ.")
    if post_name in fnames:
        citizen += " " + fh.get("post_holiday", "Первый день после праздников — трафик выше нормы весь день.")

    return citizen, official


# ── Публичный API ─────────────────────────────────────────────────────────────

def calculate_traffic_index(
    weather: dict | None = None,
    at: datetime | None = None,
) -> TrafficIndex:
    """Вычисляет синтетический индекс дорожной нагрузки.

    Args:
        weather: погодные данные — ожидаемые ключи:
                 weathercode (WMO), precipitation (мм/ч), temperature_c (°C), wind_speed_ms (м/с)
        at:      момент времени (UTC). По умолчанию — текущий момент.

    Returns:
        TrafficIndex: оценка 0–10, уровень, факторы, рекомендации.
    """
    now_utc = at or datetime.now(timezone.utc)
    nsk_dt  = now_utc + timedelta(hours=_NSK_UTC_OFFSET)
    today   = nsk_dt.date()
    dow     = nsk_dt.weekday()   # 0=Пн … 6=Вс
    hour    = nsk_dt.hour + nsk_dt.minute / 60.0

    day_type   = _HOLIDAY_DB.get(today, "")
    is_holiday = (day_type in ("holiday", "bridge")) or (dow >= 5)
    is_pre     = day_type == "pre"

    # Если завтра праздник — сегодня предпраздничный (если не уже выходной)
    if not is_holiday and not is_pre:
        tomorrow_type = _HOLIDAY_DB.get(today + timedelta(days=1), "")
        if tomorrow_type in ("holiday", "bridge"):
            is_pre = True

    # Базовый балл по времени суток
    base, time_label = _time_base(hour)
    # Выходные/праздники: трафик ≈ вдвое меньше (масштаб из traffic_rules.yaml)
    ws = float(_rules.get("traffic_rules").get("weekend_scale", 0.45))
    weekend_scale = ws if is_holiday else 1.0
    base_scaled = base * weekend_scale

    # Модификаторы дня
    day_mod, day_fac = _day_factors(dow, hour, today, is_holiday, is_pre, day_type)
    if is_holiday:
        day_mod *= 0.5  # на праздниках эффекты слабее

    # Погода
    w_delta, w_fac = _weather_factors(weather, today=today)

    raw   = base_scaled + day_mod + w_delta
    index = round(max(0.0, min(10.0, raw)), 1)

    level, emoji = _classify(index)
    next_peak    = _next_peak_desc(hour, dow, is_holiday)
    citizen, official = _build_tips(index, day_fac + w_fac)

    return TrafficIndex(
        index       = index,
        level       = level,
        emoji       = emoji,
        citizen_tip = citizen,
        official_tip= official,
        timestamp   = nsk_dt.strftime("%Y-%m-%d %H:%M NSK"),
        next_peak   = next_peak,
        factors     = day_fac + w_fac,
    )


def reload_traffic_rules() -> list[str]:
    """Перезагрузить регламенты из YAML и пересобрать кэшированные глобалы.

    Вызывается из POST /admin/reload-rules — применяет новые коэффициенты
    без перезапуска сервера.
    """
    global _HOLIDAY_DB, _ANNUAL_EVENTS, _TIME_ZONES
    global _SNOW_CODES, _RAIN_CODES, _ICE_CODES, _STORM_CODES, _FOG_CODES

    reloaded = _rules.reload()

    _HOLIDAY_DB    = _build_holiday_db()
    _ANNUAL_EVENTS = _build_annual_events()
    _TIME_ZONES    = _build_time_zones()
    _SNOW_CODES, _RAIN_CODES, _ICE_CODES, _STORM_CODES, _FOG_CODES = _build_wmo_codes()

    log.info("reload_traffic_rules: пересобраны глобалы, регламенты: %s", reloaded)
    return reloaded


def get_traffic_index_with_weather() -> dict[str, Any]:
    """Получает индекс пробок, подтягивая актуальные погодные данные из кэша.

    Приоритет данных:
      1. fact_measurements (temperature_c, wind_speed_ms) — Центральный район
      2. eco_forecast (weathercode, precipitation) — сегодняшний день
    """
    weather: dict[str, Any] = {
        "weathercode":   None,
        "precipitation": None,
        "temperature_c": None,
        "wind_speed_ms": None,
    }

    # Шаг 1: температура и ветер из текущих измерений
    try:
        from .ecology_cache import query_current
        rows = query_current()
        if rows:
            central = next(
                (r for r in rows if "централь" in (r.get("district") or "").lower()),
                rows[0],
            )
            weather["temperature_c"] = central.get("temperature_c")
            weather["wind_speed_ms"]  = central.get("wind_speed_ms")
    except Exception as e:
        log.warning("traffic_index: ошибка получения измерений: %s", e)

    # Шаг 2: weathercode и осадки из прогноза (сегодня)
    try:
        from .ecology_cache import query_forecast
        forecast = query_forecast(days=1)
        if forecast:
            today_f = forecast[0]
            if weather["weathercode"] is None:
                weather["weathercode"] = today_f.get("weathercode")
            if weather["precipitation"] is None:
                daily_mm = today_f.get("precipitation") or 0.0
                # суточная сумма → грубая оценка часовой интенсивности
                weather["precipitation"] = round(daily_mm / 16, 2) if daily_mm else 0.0
    except Exception as e:
        log.warning("traffic_index: ошибка получения прогноза: %s", e)

    ti = calculate_traffic_index(weather=weather)

    result: dict[str, Any] = {
        "index":        ti.index,
        "level":        ti.level,
        "emoji":        ti.emoji,
        "citizen_tip":  ti.citizen_tip,
        "official_tip": ti.official_tip,
        "next_peak":    ti.next_peak,
        "timestamp":    ti.timestamp,
        "factors": [
            {
                "name":        f.name,
                "description": f.description,
                "delta":       round(f.delta, 1),
                "emoji":       f.emoji,
            }
            for f in ti.factors
        ],
        "weather_used": {
            k: v for k, v in weather.items() if v is not None
        },
        "notice": (
            "⚠️ Индекс рассчитан аналитически на основе паттернов часов пик, "
            "дня недели, официальных праздников и текущих погодных условий. "
            "Реальные данные о пробках не используются."
        ),
        "source": "Аналитическая модель NSK OpenData Bot · Open-Meteo (погода)",
    }

    # Сравнение с Яндекс.Пробками (реальный индекс)
    try:
        from .yandex_traffic import fetch_yandex_traffic
        yt = fetch_yandex_traffic()
        if yt:
            result["yandex_traffic"] = yt
            diff = round(ti.index - yt["level"], 1)
            result["comparison"] = {
                "our_index":    ti.index,
                "yandex_level": yt["level"],
                "diff":         diff,
                "diff_label":   (
                    "совпадает" if abs(diff) < 1.0
                    else f"наш {'выше' if diff > 0 else 'ниже'} на {abs(diff):.1f}"
                ),
                "yandex_hint":  yt.get("hint", ""),
            }
    except Exception as e:
        log.debug("traffic_index: yandex_traffic недоступен: %s", e)

    return result
