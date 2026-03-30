"""Хранилище данных экологии и метеорологии в DuckDB.

Таблицы (ТЗ §4):
  dim_stations    — справочник станций мониторинга (10 районов Новосибирска)
  fact_measurements — журнал измерений: PM2.5, PM10, NO2, AQI, температура, ветер

Стратегия хранения:
  - История за последние ECOLOGY_HISTORY_DAYS дней (скользящее окно)
  - Upsert по составному ключу (id = station_id + timestamp) — без дублей
  - Быстрый поиск < 200 мс (индексы по station_id и measured_at)
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .cache import _get_conn
from .city_config import get_ecology_stations as _get_ecology_stations
from .constants import (
    ECOLOGY_HISTORY_DAYS,
    ECOLOGY_TTL_MINUTES,
    DATA_DIR,
)
from .rule_engine import rules as _rules


def _eco_rules() -> dict:
    """Возвращает секцию risks из ecology_rules.yaml."""
    return _rules.get("ecology_rules").get("risks", {})

log = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL_STATIONS = """
CREATE TABLE IF NOT EXISTS dim_stations (
    station_id  VARCHAR PRIMARY KEY,
    source      VARCHAR,
    latitude    DOUBLE,
    longitude   DOUBLE,
    district    VARCHAR,
    address     VARCHAR
)
"""

_DDL_MEASUREMENTS = """
CREATE TABLE IF NOT EXISTS fact_measurements (
    id                  VARCHAR PRIMARY KEY,
    station_id          VARCHAR,
    measured_at         VARCHAR,
    pm25                DOUBLE,
    pm10                DOUBLE,
    no2                 DOUBLE,
    aqi                 INTEGER,
    temperature_c       DOUBLE,
    wind_speed_ms       DOUBLE,
    wind_direction_deg  DOUBLE,
    humidity_pct        DOUBLE,
    pressure_hpa        DOUBLE,
    source              VARCHAR
)
"""

# Постоянный архив дневных агрегатов — не очищается автоматически (хранится до 365 дней).
# Ключ: (day, district) — upsert при каждом обновлении за текущий день.
_DDL_DAILY_ARCHIVE = """
CREATE TABLE IF NOT EXISTS ecology_daily_archive (
    day           VARCHAR,      -- 'YYYY-MM-DD'
    district      VARCHAR,
    pm25_avg      DOUBLE,
    pm25_max      DOUBLE,
    pm10_avg      DOUBLE,
    aqi_avg       DOUBLE,
    temp_avg      DOUBLE,
    wind_avg      DOUBLE,
    humidity_avg  DOUBLE,
    snapshots     INTEGER,
    PRIMARY KEY (day, district)
)
"""


_DDL_FORECAST = """
CREATE TABLE IF NOT EXISTS eco_forecast (
    id            VARCHAR PRIMARY KEY,   -- station_id + forecast_date
    station_id    VARCHAR,
    district      VARCHAR,
    forecast_date VARCHAR,              -- 'YYYY-MM-DD'
    temp_max      DOUBLE,
    temp_min      DOUBLE,
    wind_max      DOUBLE,
    precipitation DOUBLE,
    snowfall_cm   DOUBLE,               -- снегопад см/сутки (Open-Meteo snowfall_sum)
    weathercode   INTEGER,
    fetched_at    VARCHAR
)
"""

# WMO Weather Interpretation Codes → (иконка, описание)
WMO_WEATHER: dict[int, tuple[str, str]] = {
    0:  ("☀️",  "Ясно"),
    1:  ("🌤",  "Преим. ясно"),
    2:  ("⛅",  "Перем. облачность"),
    3:  ("☁️",  "Пасмурно"),
    45: ("🌫",  "Туман"),
    48: ("🌫",  "Иней из тумана"),
    51: ("🌦",  "Морось"),
    53: ("🌦",  "Умер. морось"),
    55: ("🌧",  "Сильная морось"),
    61: ("🌧",  "Небольшой дождь"),
    63: ("🌧",  "Умер. дождь"),
    65: ("🌧",  "Сильный дождь"),
    71: ("🌨",  "Небольшой снег"),
    73: ("🌨",  "Умер. снег"),
    75: ("❄️",  "Сильный снег"),
    77: ("🌨",  "Снежная крупа"),
    80: ("🌦",  "Ливень"),
    82: ("⛈",  "Сильный ливень"),
    85: ("🌨",  "Снегопад"),
    86: ("❄️",  "Сильный снегопад"),
    95: ("⛈",  "Гроза"),
    96: ("⛈",  "Гроза с градом"),
    99: ("⛈",  "Гроза с сильным градом"),
}


def _wmo_label(code: int | None) -> tuple[str, str]:
    """Возвращает (иконка, описание) для WMO-кода."""
    if code is None:
        return ("❓", "Нет данных")
    # Ближайший ключ для неточных кодов (некоторые API дают 82 вместо 80)
    return WMO_WEATHER.get(code, ("🌡", f"Код {code}"))


def init_ecology_tables() -> None:
    """Создаёт таблицы если их нет."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = _get_conn()
    try:
        conn.execute(_DDL_STATIONS)
        conn.execute(_DDL_MEASUREMENTS)
        conn.execute(_DDL_DAILY_ARCHIVE)
        conn.execute(_DDL_FORECAST)
        # Миграция: добавляем snowfall_cm если таблица уже существовала без неё
        try:
            conn.execute("ALTER TABLE eco_forecast ADD COLUMN snowfall_cm DOUBLE")
        except Exception:
            pass  # колонка уже есть
    finally:
        conn.close()


def _save_daily_archive(conn) -> None:
    """Пересчитывает и сохраняет агрегаты за сегодня в ecology_daily_archive.

    Вызывается после каждого upsert измерений — обновляет строку (today, district).
    Архив хранится постоянно (удаляются только записи старше 365 дней).
    """
    try:
        conn.execute("""
            DELETE FROM ecology_daily_archive
            WHERE day < STRFTIME(CURRENT_DATE - INTERVAL '365 days', '%Y-%m-%d')
        """)
        conn.execute("""
            INSERT INTO ecology_daily_archive
                (day, district, pm25_avg, pm25_max, pm10_avg, aqi_avg,
                 temp_avg, wind_avg, humidity_avg, snapshots)
            SELECT
                STRFTIME(CAST(f.measured_at AS TIMESTAMP), '%Y-%m-%d') AS day,
                s.district,
                ROUND(AVG(f.pm25), 1),
                ROUND(MAX(f.pm25), 1),
                ROUND(AVG(f.pm10), 1),
                ROUND(AVG(f.aqi), 0),
                ROUND(AVG(f.temperature_c), 1),
                ROUND(AVG(f.wind_speed_ms), 1),
                ROUND(AVG(f.humidity_pct), 0),
                COUNT(*)
            FROM fact_measurements f
            JOIN dim_stations s ON f.station_id = s.station_id
            WHERE STRFTIME(CAST(f.measured_at AS TIMESTAMP), '%Y-%m-%d')
                  = STRFTIME(CURRENT_DATE, '%Y-%m-%d')
            GROUP BY day, s.district
            ON CONFLICT (day, district) DO UPDATE SET
                pm25_avg     = excluded.pm25_avg,
                pm25_max     = excluded.pm25_max,
                pm10_avg     = excluded.pm10_avg,
                aqi_avg      = excluded.aqi_avg,
                temp_avg     = excluded.temp_avg,
                wind_avg     = excluded.wind_avg,
                humidity_avg = excluded.humidity_avg,
                snapshots    = excluded.snapshots
        """)
    except Exception as e:
        log.warning(f"Ошибка обновления daily archive: {e}")


def upsert_stations(stations: list[dict] | None = None) -> None:
    """Синхронизирует справочник станций (dim_stations).

    По умолчанию использует станции из city_profile.yaml через city_config.
    source = 'open-meteo' если станция виртуальная (по координатам района).
    """
    if stations is None:
        stations = _get_ecology_stations()

    init_ecology_tables()
    conn = _get_conn()
    try:
        for s in stations:
            conn.execute(
                """
                INSERT INTO dim_stations (station_id, source, latitude, longitude, district, address)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (station_id) DO UPDATE SET
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    district = excluded.district,
                    address = excluded.address
                """,
                [
                    s["station_id"],
                    s.get("source", "open-meteo"),
                    s["latitude"],
                    s["longitude"],
                    s["district"],
                    s.get("address", ""),
                ],
            )
    finally:
        conn.close()


def upsert_measurements(records: list[dict[str, Any]]) -> int:
    """Upsert измерений + очистка старых записей.

    Возвращает количество добавленных/обновлённых записей.
    """
    if not records:
        return 0

    init_ecology_tables()
    conn = _get_conn()
    try:
        # Удаляем записи старше ECOLOGY_HISTORY_DAYS
        cutoff = (datetime.now(timezone.utc) - timedelta(days=ECOLOGY_HISTORY_DAYS)).isoformat()
        conn.execute("DELETE FROM fact_measurements WHERE measured_at < ?", [cutoff])

        count = 0
        for r in records:
            try:
                conn.execute(
                    """
                    INSERT INTO fact_measurements
                        (id, station_id, measured_at, pm25, pm10, no2, aqi,
                         temperature_c, wind_speed_ms, wind_direction_deg,
                         humidity_pct, pressure_hpa, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        pm25 = excluded.pm25,
                        pm10 = excluded.pm10,
                        no2  = excluded.no2,
                        aqi  = excluded.aqi,
                        temperature_c = excluded.temperature_c,
                        wind_speed_ms = excluded.wind_speed_ms,
                        wind_direction_deg = excluded.wind_direction_deg,
                        humidity_pct = excluded.humidity_pct,
                        pressure_hpa = excluded.pressure_hpa,
                        source = excluded.source
                    """,
                    [
                        r["id"], r["station_id"], r["measured_at"],
                        r.get("pm25"), r.get("pm10"), r.get("no2"),
                        int(r["aqi"]) if r.get("aqi") is not None else None,
                        r.get("temperature_c"), r.get("wind_speed_ms"),
                        r.get("wind_direction_deg"), r.get("humidity_pct"),
                        r.get("pressure_hpa"), r.get("source", "open-meteo"),
                    ],
                )
                count += 1
            except Exception as e:
                log.error(f"Ошибка upsert записи {r.get('id')}: {e}")
        log.info(f"Ecology upsert: {count} записей")
        _save_daily_archive(conn)
        return count
    finally:
        conn.close()


def is_ecology_stale(ttl_minutes: int = ECOLOGY_TTL_MINUTES) -> bool:
    """True если данных нет или они устарели (для текущего города).

    Фильтрует по station_id текущего city_profile, чтобы после
    переключения города данные старого города не маскировали отсутствие
    данных нового.
    """
    try:
        init_ecology_tables()
        current_ids = [s["station_id"] for s in _get_ecology_stations()]
        if not current_ids:
            return True
        conn = _get_conn()
        try:
            placeholders = ", ".join(["?"] * len(current_ids))
            row = conn.execute(
                f"SELECT MAX(measured_at) FROM fact_measurements WHERE station_id IN ({placeholders})",
                current_ids,
            ).fetchone()
            last = row[0] if row else None
            if not last:
                return True
            last_dt = datetime.fromisoformat(last)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) - last_dt > timedelta(minutes=ttl_minutes)
        finally:
            conn.close()
    except Exception:
        return True


def get_ecology_meta() -> dict:
    """Метаданные: последнее обновление, количество записей, покрытие районов."""
    try:
        init_ecology_tables()
        conn = _get_conn()
        try:
            last = conn.execute("SELECT MAX(measured_at) FROM fact_measurements").fetchone()[0] or ""
            total = conn.execute("SELECT COUNT(*) FROM fact_measurements").fetchone()[0]
            districts = conn.execute(
                """
                SELECT COUNT(DISTINCT s.district)
                FROM fact_measurements f
                JOIN dim_stations s ON f.station_id = s.station_id
                WHERE f.measured_at = (SELECT MAX(measured_at) FROM fact_measurements)
                """
            ).fetchone()[0]
            return {
                "last_updated": last,
                "total_records": int(total),
                "districts_covered": int(districts),
            }
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Ошибка get_ecology_meta: {e}")
        return {"last_updated": "", "total_records": 0, "districts_covered": 0}


def query_current(district_filter: str | None = None) -> list[dict]:
    """Текущие показатели (последний снимок) по всем или одному району.

    Интент ТЗ §5: «Текущее качество воздуха по районам».
    SQL: AVG(aqi) GROUP BY district за последний снимок (< 1 ч).
    """
    init_ecology_tables()
    conn = _get_conn()
    try:
        wheres = ["f.measured_at = (SELECT MAX(measured_at) FROM fact_measurements)"]
        params: list = []
        if district_filter:
            wheres.append("s.district ILIKE ?")
            params.append(f"%{district_filter.split()[0]}%")
        where_sql = "WHERE " + " AND ".join(wheres)
        sql = f"""
            SELECT
                s.district,
                s.address,
                ROUND(f.pm25, 1)          AS pm25,
                ROUND(f.pm10, 1)          AS pm10,
                ROUND(f.no2,  1)          AS no2,
                f.aqi,
                ROUND(f.temperature_c, 1) AS temperature_c,
                ROUND(f.wind_speed_ms, 1) AS wind_speed_ms,
                ROUND(f.humidity_pct, 0)  AS humidity_pct,
                f.source,
                f.measured_at
            FROM fact_measurements f
            JOIN dim_stations s ON f.station_id = s.station_id
            {where_sql}
            ORDER BY f.aqi DESC NULLS LAST, s.district
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка query_current: {e}")
        return []
    finally:
        conn.close()


def query_pdk_exceedances(district_filter: str | None = None) -> list[dict]:
    """Записи за сегодня, где PM2.5 > 35 мкг/м³ (порог ВОЗ, ТЗ §5).

    Интент: «Превышение ПДК».
    """
    init_ecology_tables()
    conn = _get_conn()
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        pdk_thr = float(_eco_rules().get("pdk", {}).get("threshold", 35.0))
        wheres = ["f.measured_at >= ?", f"f.pm25 > {pdk_thr}"]
        params: list = [today]
        if district_filter:
            wheres.append("s.district ILIKE ?")
            params.append(f"%{district_filter.split()[0]}%")
        where_sql = "WHERE " + " AND ".join(wheres)
        sql = f"""
            SELECT
                s.district,
                ROUND(MAX(f.pm25), 1) AS pm25_max,
                ROUND(AVG(f.pm25), 1) AS pm25_avg,
                COUNT(*)              AS измерений,
                MAX(f.measured_at)    AS последнее
            FROM fact_measurements f
            JOIN dim_stations s ON f.station_id = s.station_id
            {where_sql}
            GROUP BY s.district
            ORDER BY pm25_max DESC
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка query_pdk_exceedances: {e}")
        return []
    finally:
        conn.close()


def query_risks(district_filter: str | None = None) -> list[dict]:
    """Вычисляет риски на основе текущих данных + суточной дельты температуры.

    Сценарии (ТЗ §2):
      smog_trap    — ветер < 1.5 м/с + PM2.5 > 20 мкг/м³ (экологическая ловушка)
      pdk          — PM2.5 > 35 мкг/м³ (превышение нормы ВОЗ)
      ice          — температура в диапазоне -3..+2°C (риск чёрного льда)
      temp_shock   — суточная дельта температуры ≤ −15°C (температурный шок)
      severe_cold  — температура ниже −20°C (экстремальный холод)
    """
    from datetime import date, timedelta

    rows = query_current(district_filter)
    if not rows:
        return []

    # Агрегированные метрики по всем выбранным районам
    def _vals(key):
        return [r[key] for r in rows if r.get(key) is not None]

    temps = _vals("temperature_c")
    winds = _vals("wind_speed_ms")
    pm25s = _vals("pm25")
    hums  = _vals("humidity_pct")

    avg = lambda lst: round(sum(lst) / len(lst), 1) if lst else None
    avg_temp = avg(temps)
    avg_wind = avg(winds)
    avg_pm25 = avg(pm25s)
    avg_hum  = round(sum(hums) / len(hums)) if hums else None
    min_temp = round(min(temps), 1) if temps else None

    # 24-часовая дельта температуры через query_history
    temp_delta_24h: float | None = None
    try:
        history = query_history(district_filter=district_filter, days=2)
        today_str = str(date.today())
        yest_str  = str(date.today() - timedelta(days=1))
        t_today = [r["темп_ср"] for r in history
                   if str(r.get("день", ""))[:10] == today_str and r.get("темп_ср") is not None]
        t_yest  = [r["темп_ср"] for r in history
                   if str(r.get("день", ""))[:10] == yest_str  and r.get("темп_ср") is not None]
        if t_today and t_yest:
            temp_delta_24h = round(sum(t_today) / len(t_today) - sum(t_yest) / len(t_yest), 1)
    except Exception:
        pass

    risks: list[dict] = []

    # Загружаем пороги из ecology_rules.yaml
    _r = _eco_rules()
    _smog     = _r.get("smog_trap",    {})
    _pdk      = _r.get("pdk",          {})
    _ice      = _r.get("black_ice",    {})
    _shock    = _r.get("temp_shock",   {})
    _cold     = _r.get("extreme_cold", {})

    smog_wind_thr    = float(_smog.get("wind_threshold_ms",         1.5))
    smog_warn_pm25   = float(_smog.get("pm25_warning_threshold",   20.0))
    smog_crit_pm25   = float(_smog.get("pm25_critical_threshold",  35.0))
    pdk_thr          = float(_pdk.get("threshold",                 35.0))
    ice_t_min        = float(_ice.get("temp_min",                  -3.0))
    ice_t_max        = float(_ice.get("temp_max",                   2.0))
    shock_delta      = float(_shock.get("delta_threshold",        -15.0))
    cold_warn_thr    = float(_cold.get("warning_threshold",       -20.0))
    cold_crit_thr    = float(_cold.get("critical_threshold",      -30.0))

    # ── Сценарий В: Экологическая ловушка / Чёрное небо ──────────────────────
    if avg_wind is not None and avg_pm25 is not None and avg_wind < smog_wind_thr and avg_pm25 > smog_warn_pm25:
        severity = "critical" if avg_pm25 > smog_crit_pm25 else "warning"
        risks.append({
            "id": "smog_trap",
            "scenario": "Экологическая ловушка",
            "severity": severity,
            "icon": "🌫️",
            "title": "Безветрие блокирует рассеивание выбросов",
            "metrics": f"Ветер {avg_wind} м/с · PM2.5 {avg_pm25} мкг/м³",
            "citizen": (
                "Не открывайте окна для проветривания ночью. "
                "Включите домашние очистители воздуха. "
                "Отложите пробежки и интенсивные занятия на улице."
            ),
            "official": (
                "Рассмотреть объявление режима НМУ (неблагоприятных метеоусловий). "
                "Выпустить предписания предприятиям снизить выбросы на 15–20%."
            ),
        })

    # ── Превышение нормы ВОЗ по PM2.5 ────────────────────────────────────────
    if avg_pm25 is not None and avg_pm25 > pdk_thr:
        risks.append({
            "id": "pdk",
            "scenario": "Превышение нормы ВОЗ",
            "severity": "critical",
            "icon": "☢️",
            "title": f"PM2.5 = {avg_pm25} мкг/м³ — норма превышена в {round(avg_pm25 / pdk_thr, 1)}×",
            "metrics": f"Норма ВОЗ: {pdk_thr} мкг/м³ · Текущее: {avg_pm25} мкг/м³",
            "citizen": (
                "Ограничьте время на улице. "
                "Носите маску класса FFP2 / N95. "
                "Не проветривайте помещение, закройте окна."
            ),
            "official": (
                "Задействовать систему экстренного оповещения населения. "
                "Рекомендовать отмену открытых массовых мероприятий."
            ),
        })

    # ── Сценарий А: Риск чёрного льда ────────────────────────────────────────
    if avg_temp is not None and ice_t_min <= avg_temp <= ice_t_max:
        hum_note = f" · Влажность {avg_hum}%" if avg_hum else ""
        risks.append({
            "id": "ice",
            "scenario": "Риск гололёда",
            "severity": "warning",
            "icon": "🧊",
            "title": f"Температура {avg_temp:+.1f}°C — зона риска чёрного льда",
            "metrics": f"Температура {avg_temp:+.1f}°C{hum_note}",
            "citizen": (
                "Опасность скрытого обледенения дорог и тротуаров. "
                "Закладывайте +20–30 минут на маршрут. "
                "Пешеходам — избегать крутых спусков, рассмотрите метро вместо авто."
            ),
            "official": (
                "Превентивно вывести пескоразбрасывающую технику на мосты и магистрали. "
                "Обработать пешеходные зоны у больниц, школ, остановок."
            ),
        })

    # ── Сценарий Б: Температурный шок ────────────────────────────────────────
    if temp_delta_24h is not None and temp_delta_24h <= shock_delta:
        risks.append({
            "id": "temp_shock",
            "scenario": "Температурный шок",
            "severity": "critical",
            "icon": "⚠️",
            "title": f"Резкое похолодание на {abs(temp_delta_24h):.0f}°C за сутки",
            "metrics": f"Дельта температуры за 24 ч: {temp_delta_24h:+.1f}°C",
            "citizen": (
                "Прогрейте автомобиль с вечера — есть риск не завестись утром. "
                "Младшие классы школ возможно перейдут на дистант. "
                "Одевайтесь многослойно."
            ),
            "official": (
                "Требуется резкое повышение температуры теплоносителя на ТЭЦ. "
                "Максимальный риск порывов в изношенных трубопроводах (Ленинский, Кировский р-н). "
                "Аварийные бригады ЖКХ — режим повышенной готовности."
            ),
        })

    # ── Экстремальный холод ───────────────────────────────────────────────────
    if min_temp is not None and min_temp < cold_warn_thr:
        risks.append({
            "id": "severe_cold",
            "scenario": "Экстремальный холод",
            "severity": "critical" if min_temp < cold_crit_thr else "warning",
            "icon": "🥶",
            "title": f"Экстремальные морозы: до {min_temp:.0f}°C",
            "metrics": f"Минимум по районам: {min_temp:.0f}°C",
            "citizen": (
                "Ограничьте нахождение на улице. "
                "Особое внимание: пожилые, дети, домашние животные. "
                "Прогрейте автомобиль заранее."
            ),
            "official": (
                "Открыть пункты обогрева и ночлежки. "
                "Аварийные бригады — дежурный режим. "
                "Контролировать теплоснабжение социальных объектов (школы, больницы, дома престарелых)."
            ),
        })

    return risks


def seed_history_placeholder(
    days: int = 20,
    temp_c: float = -10.0,
) -> int:
    """Заполняет ecology_daily_archive историческими данными-заглушками.

    Вставляет строки только для тех (day, district), которых ещё нет в архиве
    (ON CONFLICT DO NOTHING). Реальные данные от Open-Meteo автоматически
    перезапишут заглушки при очередном обновлении (ON CONFLICT DO UPDATE).

    Используется при первом запуске, чтобы функция «история за N дней»
    возвращала осмысленные данные сразу, а не только сегодняшние.

    Args:
        days: сколько дней заполнить назад от сегодня.
        temp_c: температура-заглушка (°C).
    Returns:
        Количество вставленных строк.
    """
    init_ecology_tables()
    conn = _get_conn()
    inserted = 0
    _stations = _get_ecology_stations()
    try:
        today = datetime.now(timezone.utc).date()
        for i in range(1, days + 1):          # не трогаем сегодня — будет реальное
            day_str = str(today - timedelta(days=i))
            for st in _stations:
                try:
                    conn.execute(
                        """
                        INSERT INTO ecology_daily_archive
                            (day, district, pm25_avg, pm25_max, pm10_avg, aqi_avg,
                             temp_avg, wind_avg, humidity_avg, snapshots)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (day, district) DO NOTHING
                        """,
                        [
                            day_str,
                            st["district"],
                            12.0,   # pm25_avg — типичная зима, ниже ПДК
                            18.0,   # pm25_max
                            22.0,   # pm10_avg
                            42,     # aqi_avg — «Хорошо»
                            temp_c, # temp_avg
                            3.5,    # wind_avg м/с
                            78.0,   # humidity_avg %
                            0,      # snapshots=0 означает заглушку
                        ],
                    )
                    inserted += 1
                except Exception as e:
                    log.debug(f"seed_history_placeholder skip {day_str}/{st['district']}: {e}")
        log.info("seed_history_placeholder: вставлено %d строк (%d дней × %d станций)",
                 inserted, days, len(_stations))
        return inserted
    finally:
        conn.close()


def load_ecology_seed() -> int:
    """Загружает данные из ecology_seed.json в ecology_daily_archive.

    Файл создаётся вручную (экспорт из локальной БД) и коммитится в git.
    При деплое на Railway/Render данные загружаются при первом запуске,
    обеспечивая полноценный отчёт с первого дня.

    ON CONFLICT DO UPDATE — реальные данные (snapshots > 1) заменяют заглушки.
    """
    import json
    from pathlib import Path

    init_ecology_tables()

    # Ищем seed-файл для текущего города
    city_id = "novosibirsk"
    try:
        from .city_config import get_city
        city_id = get_city().get("id", "novosibirsk")
    except Exception:
        pass

    seed_path = Path(DATA_DIR) / "cities" / city_id / "ecology_seed.json"
    if not seed_path.exists():
        return 0

    try:
        with open(seed_path, encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        log.warning("load_ecology_seed: не удалось прочитать %s: %s", seed_path, e)
        return 0

    records = payload.get("records", [])
    if not records:
        return 0

    conn = _get_conn()
    inserted = 0
    try:
        for r in records:
            try:
                conn.execute(
                    """
                    INSERT INTO ecology_daily_archive
                        (day, district, pm25_avg, pm25_max, pm10_avg, aqi_avg,
                         temp_avg, wind_avg, humidity_avg, snapshots)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (day, district) DO UPDATE SET
                        pm25_avg = CASE WHEN excluded.snapshots > ecology_daily_archive.snapshots
                                        THEN excluded.pm25_avg ELSE ecology_daily_archive.pm25_avg END,
                        pm25_max = CASE WHEN excluded.snapshots > ecology_daily_archive.snapshots
                                        THEN excluded.pm25_max ELSE ecology_daily_archive.pm25_max END,
                        pm10_avg = CASE WHEN excluded.snapshots > ecology_daily_archive.snapshots
                                        THEN excluded.pm10_avg ELSE ecology_daily_archive.pm10_avg END,
                        aqi_avg = CASE WHEN excluded.snapshots > ecology_daily_archive.snapshots
                                       THEN excluded.aqi_avg ELSE ecology_daily_archive.aqi_avg END,
                        temp_avg = CASE WHEN excluded.snapshots > ecology_daily_archive.snapshots
                                        THEN excluded.temp_avg ELSE ecology_daily_archive.temp_avg END,
                        wind_avg = CASE WHEN excluded.snapshots > ecology_daily_archive.snapshots
                                        THEN excluded.wind_avg ELSE ecology_daily_archive.wind_avg END,
                        humidity_avg = CASE WHEN excluded.snapshots > ecology_daily_archive.snapshots
                                            THEN excluded.humidity_avg ELSE ecology_daily_archive.humidity_avg END,
                        snapshots = CASE WHEN excluded.snapshots > ecology_daily_archive.snapshots
                                         THEN excluded.snapshots ELSE ecology_daily_archive.snapshots END
                    """,
                    [
                        r["day"], r["district"],
                        r.get("pm25_avg", 0), r.get("pm25_max", 0),
                        r.get("pm10_avg", 0), r.get("aqi_avg", 0),
                        r.get("temp_avg", 0), r.get("wind_avg", 0),
                        r.get("humidity_avg", 0), r.get("snapshots", 0),
                    ],
                )
                inserted += 1
            except Exception as e:
                log.debug("load_ecology_seed skip %s/%s: %s", r.get("day"), r.get("district"), e)

        log.info("load_ecology_seed: загружено %d из %d записей из %s", inserted, len(records), seed_path)
        return inserted
    finally:
        conn.close()


def upsert_forecast(records: list[dict]) -> int:
    """Сохраняет 7-дневный прогноз погоды в eco_forecast.

    Перезаписывает существующие записи по (station_id, forecast_date).
    """
    if not records:
        return 0
    init_ecology_tables()
    conn = _get_conn()
    try:
        count = 0
        for r in records:
            try:
                conn.execute(
                    """
                    INSERT INTO eco_forecast
                        (id, station_id, district, forecast_date,
                         temp_max, temp_min, wind_max, precipitation, snowfall_cm, weathercode, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        temp_max      = excluded.temp_max,
                        temp_min      = excluded.temp_min,
                        wind_max      = excluded.wind_max,
                        precipitation = excluded.precipitation,
                        snowfall_cm   = excluded.snowfall_cm,
                        weathercode   = excluded.weathercode,
                        fetched_at    = excluded.fetched_at
                    """,
                    [
                        r["id"], r["station_id"], r.get("district", ""),
                        r["forecast_date"],
                        r.get("temp_max"), r.get("temp_min"),
                        r.get("wind_max"), r.get("precipitation"),
                        r.get("snowfall_cm"), r.get("weathercode"), r["fetched_at"],
                    ],
                )
                count += 1
            except Exception as e:
                log.error(f"Forecast upsert error {r.get('id')}: {e}")
        log.info(f"Forecast upsert: {count} записей")
        return count
    finally:
        conn.close()


def is_forecast_stale(ttl_hours: int = 6) -> bool:
    """True если прогноз не обновлялся более ttl_hours часов или отсутствует."""
    try:
        init_ecology_tables()
        conn = _get_conn()
        try:
            row = conn.execute("SELECT MAX(fetched_at) FROM eco_forecast").fetchone()
            last = row[0] if row else None
            if not last:
                return True
            last_dt = datetime.fromisoformat(last)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) - last_dt > timedelta(hours=ttl_hours)
        finally:
            conn.close()
    except Exception:
        return True


def query_forecast(district_filter: str | None = None, days: int = 7) -> list[dict]:
    """Возвращает 7-дневный прогноз погоды агрегированный по дням.

    Если district_filter задан — только данные по этому району.
    Иначе — среднее по всем районам (агрегированное по городу).

    Возвращает список dict с полями:
      forecast_date, day_name, temp_max, temp_min, wind_max,
      precipitation, snowfall_cm, weathercode, weather_icon, weather_desc,
      ice_risk, cold_risk, snow_risk, snow_impact
    """
    init_ecology_tables()
    conn = _get_conn()
    try:
        today = str(datetime.now(timezone.utc).date())
        cutoff = str((datetime.now(timezone.utc) + timedelta(days=days)).date())

        wheres = ["forecast_date > ?", "forecast_date <= ?"]
        params: list = [today, cutoff]
        if district_filter:
            d = district_filter.split()[0]
            wheres.append("district ILIKE ?")
            params.append(f"%{d}%")

        where_sql = "WHERE " + " AND ".join(wheres)
        sql = f"""
            SELECT
                forecast_date,
                ROUND(AVG(temp_max), 1)      AS temp_max,
                ROUND(AVG(temp_min), 1)      AS temp_min,
                ROUND(MAX(wind_max), 1)      AS wind_max,
                ROUND(AVG(precipitation), 1) AS precipitation,
                ROUND(AVG(snowfall_cm), 1)   AS snowfall_cm,
                CAST(ROUND(AVG(weathercode)) AS INTEGER) AS weathercode
            FROM eco_forecast
            {where_sql}
            GROUP BY forecast_date
            ORDER BY forecast_date
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        # Обогащаем каждый день: иконка, описание, флаги рисков
        ru_days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        ru_months = ["янв", "фев", "мар", "апр", "май", "июн",
                     "июл", "авг", "сен", "окт", "ноя", "дек"]
        from datetime import date as _date
        for r in rows:
            dt = _date.fromisoformat(r["forecast_date"])
            r["day_name"] = f"{ru_days[dt.weekday()]} {dt.day} {ru_months[dt.month - 1]}"
            icon, desc = _wmo_label(r["weathercode"])
            r["weather_icon"] = icon
            r["weather_desc"] = desc
            t_max = r["temp_max"]
            t_min = r["temp_min"]
            # Прогнозные флаги рисков из ecology_rules.yaml
            _fc    = _rules.get("ecology_rules").get("forecast", {})
            _ice_z = _fc.get("ice_risk", {})
            _ice_z_min = float(_ice_z.get("temp_zone_min", -3.0))
            _ice_z_max = float(_ice_z.get("temp_zone_max",  2.0))
            _cold_thr  = float(_fc.get("cold_risk", {}).get("threshold", -20.0))
            _snow_wc   = _fc.get("snow_risk", {})
            _snow_min  = int(_snow_wc.get("wmo_code_min", 70))
            _snow_max  = int(_snow_wc.get("wmo_code_max", 86))

            # Риск гололёда: суточный диапазон проходит через 0°C
            r["ice_risk"] = bool(t_min is not None and t_max is not None
                                 and t_min <= 0 <= t_max or
                                 (t_min is not None and _ice_z_min <= t_min <= _ice_z_max))
            # Риск сильного мороза
            r["cold_risk"] = bool(t_min is not None and t_min < _cold_thr)
            # Риск снегопада: осадки + код снега
            wc = r.get("weathercode") or 0
            r["snow_risk"] = bool(r.get("precipitation", 0) and _snow_min <= wc <= _snow_max)

            # Индекс влияния снегопада (0–10)
            _snow_cfg = _fc.get("snow_impact", {})
            sf = r.get("snowfall_cm") or 0.0
            regional_k = float(_snow_cfg.get("regional_coefficient", 1.0))
            sf_adj = sf * regional_k  # скорректированный снегопад
            # Формула: base (по суточному объёму) + длительность
            if sf_adj >= float(_snow_cfg.get("heavy_cm", 15)):
                snow_base = int(_snow_cfg.get("heavy_score", 8))
            elif sf_adj >= float(_snow_cfg.get("moderate_cm", 5)):
                snow_base = int(_snow_cfg.get("moderate_score", 5))
            elif sf_adj >= float(_snow_cfg.get("light_cm", 1)):
                snow_base = int(_snow_cfg.get("light_score", 2))
            else:
                snow_base = 0
            r["snow_impact"] = min(10, snow_base)
            r["snowfall_cm"] = sf

        # Второй проход: учёт длительности (суммарный снег за окно)
        _snow_cfg = _rules.get("ecology_rules").get("forecast", {}).get("snow_impact", {})
        window = int(_snow_cfg.get("accumulation_days", 3))
        acc_heavy = float(_snow_cfg.get("accumulation_heavy_cm", 20))
        acc_mod   = float(_snow_cfg.get("accumulation_moderate_cm", 10))
        regional_k = float(_snow_cfg.get("regional_coefficient", 1.0))
        for i, r in enumerate(rows):
            # сумма снегопада за окно (текущий + предыдущие дни в прогнозе)
            start = max(0, i - window + 1)
            acc_snow = sum((rows[j].get("snowfall_cm") or 0) for j in range(start, i + 1)) * regional_k
            if acc_snow >= acc_heavy:
                r["snow_impact"] = min(10, max(r["snow_impact"], 9))
            elif acc_snow >= acc_mod:
                r["snow_impact"] = min(10, max(r["snow_impact"], 6))

        return rows
    except Exception as e:
        log.error(f"Ошибка query_forecast: {e}")
        return []
    finally:
        conn.close()


def query_history(district_filter: str | None = None, days: int = 7) -> list[dict]:
    """История по дням за N дней с агрегацией по показателям.

    Интент ТЗ §5: «Динамика PM2.5 в Советском районе за неделю».
    Также даёт корреляцию wind_speed_ms vs pm25 (§5 — влияние погоды на смог).

    Стратегия: данные за последние ECOLOGY_HISTORY_DAYS дней берутся из
    fact_measurements (полные снимки); более старые данные — из
    ecology_daily_archive (постоянный архив дневных агрегатов, до 365 дней).
    """
    init_ecology_tables()
    conn = _get_conn()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cutoff_day = str((datetime.now(timezone.utc) - timedelta(days=days)).date())

        dist_filter_raw = district_filter.split()[0] if district_filter else None

        # ── Данные из скользящего окна (fact_measurements) ───────────────────
        wheres = ["f.measured_at >= ?"]
        params: list = [cutoff]
        if dist_filter_raw:
            wheres.append("s.district ILIKE ?")
            params.append(f"%{dist_filter_raw}%")
        where_sql = "WHERE " + " AND ".join(wheres)
        sql_recent = f"""
            SELECT
                STRFTIME(CAST(f.measured_at AS TIMESTAMP), '%Y-%m-%d') AS день,
                s.district                                              AS район,
                ROUND(AVG(f.pm25), 1)           AS pm25_ср,
                ROUND(MAX(f.pm25), 1)           AS pm25_макс,
                ROUND(AVG(f.aqi), 0)            AS aqi_ср,
                ROUND(AVG(f.temperature_c), 1)  AS темп_ср,
                ROUND(AVG(f.wind_speed_ms), 1)  AS ветер_ср,
                COUNT(*)                        AS снимков
            FROM fact_measurements f
            JOIN dim_stations s ON f.station_id = s.station_id
            {where_sql}
            GROUP BY день, район
        """
        cursor = conn.execute(sql_recent, params)
        cols = [d[0] for d in cursor.description]
        recent_rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        recent_days = {(r["день"], r["район"]) for r in recent_rows}

        # ── Данные из постоянного архива (ecology_daily_archive) ─────────────
        # Всегда дополняем fact_measurements данными архива (заполняем пробелы).
        # Архив обновляется ежедневно при каждом upsert измерений.
        archive_rows: list[dict] = []
        archive_wheres = ["a.day >= ?"]
        archive_params: list = [cutoff_day]
        if dist_filter_raw:
            archive_wheres.append("a.district ILIKE ?")
            archive_params.append(f"%{dist_filter_raw}%")
        archive_where_sql = "WHERE " + " AND ".join(archive_wheres)
        sql_archive = f"""
            SELECT
                a.day          AS день,
                a.district     AS район,
                a.pm25_avg     AS pm25_ср,
                a.pm25_max     AS pm25_макс,
                a.aqi_avg      AS aqi_ср,
                a.temp_avg     AS темп_ср,
                a.wind_avg     AS ветер_ср,
                a.snapshots    AS снимков
            FROM ecology_daily_archive a
            {archive_where_sql}
        """
        cur2 = conn.execute(sql_archive, archive_params)
        cols2 = [d[0] for d in cur2.description]
        for row in cur2.fetchall():
            d = dict(zip(cols2, row))
            if (d["день"], d["район"]) not in recent_days:
                archive_rows.append(d)

        all_rows = recent_rows + archive_rows
        all_rows.sort(key=lambda r: (r["день"], r["район"]), reverse=False)
        all_rows.sort(key=lambda r: r["день"], reverse=True)
        return all_rows
    except Exception as e:
        log.error(f"Ошибка query_history: {e}")
        return []
    finally:
        conn.close()


def query_aqi_exceedance_history(
    aqi_threshold: int = 40,
    days: int = 30,
    district_filter: str | None = None,
) -> list[dict]:
    """Почасовая история превышений AQI за N дней.

    Возвращает записи, где AQI >= aqi_threshold, сгруппированные по дням и часам.
    Используется для построения паттернов загрязнения (утро/вечер, дни недели).
    """
    init_ecology_tables()
    conn = _get_conn()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        wheres = ["f.measured_at >= ?", f"f.aqi >= {int(aqi_threshold)}"]
        params: list = [cutoff]
        if district_filter:
            wheres.append("s.district ILIKE ?")
            params.append(f"%{district_filter.split()[0]}%")
        where_sql = "WHERE " + " AND ".join(wheres)

        sql = f"""
            SELECT
                STRFTIME(CAST(f.measured_at AS TIMESTAMP), '%Y-%m-%d') AS day,
                EXTRACT(HOUR FROM CAST(f.measured_at AS TIMESTAMP))   AS hour,
                s.district,
                ROUND(AVG(f.aqi), 0)   AS aqi_avg,
                ROUND(MAX(f.aqi), 0)   AS aqi_max,
                ROUND(AVG(f.pm25), 1)  AS pm25_avg,
                ROUND(AVG(f.wind_speed_ms), 1) AS wind_avg,
                ROUND(AVG(f.temperature_c), 1) AS temp_avg,
                COUNT(*)               AS measurements
            FROM fact_measurements f
            JOIN dim_stations s ON f.station_id = s.station_id
            {where_sql}
            GROUP BY day, hour, s.district
            ORDER BY day DESC, hour, s.district
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка query_aqi_exceedance_history: {e}")
        return []
    finally:
        conn.close()
