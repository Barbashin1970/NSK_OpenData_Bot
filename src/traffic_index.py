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

log = logging.getLogger(__name__)

_NSK_UTC_OFFSET = 7  # UTC+7

# ── База официальных нерабочих дней 2025–2027 ────────────────────────────────
# Источники: Производственный календарь РФ, Постановления Правительства
#
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

# Дополнительные городские события (ежегодные, дата фиксирована)
# Формат: (month, day, label, traffic_delta)
_ANNUAL_EVENTS: list[tuple[int, int, str, float]] = [
    (9,  1,  "1 Сентября", +2.0),    # День знаний: все везут детей в школы
    (6, 25,  "Выпускной вечер", +1.0),  # Вечер выпускников
    (12, 28, "Канун новогодних каникул", +1.5),  # Все едут за подарками и в аэропорт
    (12, 29, "Предновогодняя суета", +2.0),
    (12, 30, "Предновогодняя суета", +2.0),
]

# WMO weathercode → категория осадков
_SNOW_CODES  = {71, 73, 75, 77, 85, 86}
_RAIN_CODES  = {51, 53, 55, 61, 63, 65, 80, 81, 82}
_ICE_CODES   = {56, 57, 66, 67}
_STORM_CODES = {95, 96, 99}
_FOG_CODES   = {45, 48}


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


# ── Паттерны часов пик ────────────────────────────────────────────────────────

# (start_hour, end_hour, base_score, label)
_TIME_ZONES = [
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
    """Модификаторы по дню недели и специальным событиям."""
    factors: list[TrafficFactor] = []
    mod = 0.0

    # Понедельник: все сонные едут на работу → сильнее по утрам
    if dow == 0 and not is_holiday and 7.0 <= hour < 10.0:
        mod += 0.8
        factors.append(TrafficFactor(
            "Понедельничный эффект",
            "Все возвращаются к работе после выходных — движение плотнее",
            0.8, "😴",
        ))

    # Пятница: вечерний исход усилен
    if dow == 4 and not is_holiday and hour >= 16.0:
        mod += 0.7
        factors.append(TrafficFactor(
            "Пятничный исход",
            "Все спешат покинуть город на выходные — вечерний пик усилен",
            0.7, "🏁",
        ))

    # Предпраздничный день: часть людей уезжает раньше → лёгкое снижение
    if is_pre and not is_holiday:
        mod -= 0.4
        factors.append(TrafficFactor(
            "Предпраздничный день",
            "Укороченный рабочий день — многие уезжают раньше обычного",
            -0.4, "🎉",
        ))

    # Пост-праздник: возврат из каникул (Jan 9 и аналоги)
    if day_type == "post_holiday":
        mod += 1.2
        factors.append(TrafficFactor(
            "Возврат после каникул",
            "Все возвращаются в город после длинных праздников — пробки выше нормы",
            1.2, "🏠➡🏙",
        ))

    # Ежегодные городские события
    for ev_month, ev_day, ev_label, ev_delta in _ANNUAL_EVENTS:
        if today.month == ev_month and today.day == ev_day:
            # 1 сентября эффективно только утром
            if ev_label == "1 Сентября" and not (7.0 <= hour < 10.0):
                continue
            mod += ev_delta
            factors.append(TrafficFactor(ev_label, f"Городское событие: {ev_label}", ev_delta, "📅"))

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

    # ── Снег ─────────────────────────────────────────────────────────────────
    if wcode in _SNOW_CODES:
        if precip > 5:
            delta += 2.5
            factors.append(TrafficFactor(
                "Сильный снегопад",
                f"Осадки {precip:.1f} мм — видимость снижена, дороги скользкие",
                2.5, "❄️",
            ))
        else:
            delta += 1.5
            factors.append(TrafficFactor(
                "Снегопад", "Снег на дорогах — движение замедляется", 1.5, "🌨",
            ))

        # Первый осенний снег (октябрь–ноябрь): город не готов
        if today.month in (10, 11):
            delta += 1.5
            factors.append(TrafficFactor(
                "Первый осенний снег",
                "Летняя резина, неубранный снег, неготовность служб — экстремальный риск",
                1.5, "🚨",
            ))

    # ── Дождь ────────────────────────────────────────────────────────────────
    elif wcode in _RAIN_CODES:
        if precip > 10:
            delta += 1.5
            factors.append(TrafficFactor(
                "Ливень", f"Интенсивные осадки {precip:.1f} мм — видимость и сцепление снижены", 1.5, "⛈",
            ))
        else:
            delta += 0.8
            factors.append(TrafficFactor(
                "Дождь", "Мокрое покрытие — водители снижают скорость", 0.8, "🌧",
            ))

    # ── Ледяной дождь / изморозь ──────────────────────────────────────────────
    elif wcode in _ICE_CODES:
        delta += 2.5
        factors.append(TrafficFactor(
            "Ледяной дождь", "Гололедица на дорогах — крайне опасно", 2.5, "🧊",
        ))

    # ── Гроза ────────────────────────────────────────────────────────────────
    elif wcode in _STORM_CODES:
        delta += 2.0
        factors.append(TrafficFactor(
            "Гроза", "Непогода резко снижает видимость и скорость потока", 2.0, "⛈",
        ))

    # ── Туман ────────────────────────────────────────────────────────────────
    elif wcode in _FOG_CODES:
        delta += 0.8
        factors.append(TrafficFactor(
            "Туман", "Видимость снижена — водители едут осторожнее", 0.8, "🌫",
        ))

    # ── Гололедица (температура ≈0 + осадки) ────────────────────────────────
    if -3 <= temp <= 2 and precip > 0 and wcode not in _ICE_CODES:
        delta += 1.0
        factors.append(TrafficFactor(
            "Риск гололедицы",
            f"Температура {temp:.0f}°C + осадки — возможна гололедица на мостах и тенистых участках",
            1.0, "🧊",
        ))

    # ── Экстремальный мороз: машины не заводятся, меньше автомобилей ─────────
    if temp < -30:
        delta -= 1.5
        factors.append(TrafficFactor(
            "Экстремальный мороз",
            f"{temp:.0f}°C — массовый отказ техники, горожане пересаживаются на ОТ",
            -1.5, "🥶",
        ))
    elif temp < -20:
        delta -= 0.5
        factors.append(TrafficFactor(
            "Сильный мороз",
            f"{temp:.0f}°C — часть авто не заводится, трафик ниже среднего",
            -0.5, "🥶",
        ))

    # ── Сильный ветер ────────────────────────────────────────────────────────
    if wind > 15:
        delta += 0.5
        factors.append(TrafficFactor(
            "Сильный ветер", f"{wind:.0f} м/с — снижение скорости на открытых участках", 0.5, "💨",
        ))

    return delta, factors


def _classify(index: float) -> tuple[str, str]:
    if index < 2.0:
        return "Свободно", "🟢"
    elif index < 3.5:
        return "Умеренно", "🟡"
    elif index < 5.0:
        return "Затруднено", "🟠"
    elif index < 6.5:
        return "Сложно", "🔴"
    elif index < 8.5:
        return "Очень сложно", "🔴"
    else:
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

    if index < 2:
        citizen  = "Дороги свободны. Отличное время для поездки."
        official = "Штатный режим. Усиления не требуется."
    elif index < 3.5:
        citizen  = "Движение умеренное. Поездка комфортная."
        official = "Штатный мониторинг."
    elif index < 5.0:
        citizen  = "Возможны заторы. Заложите дополнительно 15–20 минут."
        official = "Рекомендуется мониторинг ключевых перекрёстков."
    elif index < 6.5:
        citizen  = "Значительные заторы. Рассмотрите общественный транспорт или метро."
        official = "Рассмотреть регулировку светофоров на пиковых узлах."
    elif index < 8.5:
        citizen  = "Серьёзные пробки. Используйте метро, электросамокаты или пешие маршруты."
        official = "Задействовать ручное управление движением на перегруженных узлах."
    else:
        citizen  = "Коллапс. Оставайтесь дома или используйте только метро и пешеходные маршруты."
        official = "Режим чрезвычайной нагрузки. Аварийное регулирование. Экстренное информирование населения."

    # Специфические уточнения
    if "Первый осенний снег" in fnames:
        citizen += " ⚠️ Первый снег — экстремально скользко! Пересядьте на метро."
    if "Ледяной дождь" in fnames or "Риск гололедицы" in fnames:
        citizen += " 🧊 Гололедица: при поездке снизьте скорость вдвое."
    if "Понедельничный эффект" in fnames:
        citizen += " Выезжайте до 07:30 или после 09:30."
    if "Пятничный исход" in fnames:
        citizen += " Пятничный вечер: выезжайте после 20:00 или используйте ОТ."
    if "Возврат после каникул" in fnames:
        citizen += " Первый день после праздников — трафик выше нормы весь день."

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
    # Выходные/праздники: трафик ≈ вдвое меньше
    weekend_scale = 0.45 if is_holiday else 1.0
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

    return {
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
