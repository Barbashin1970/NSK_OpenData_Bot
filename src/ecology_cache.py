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


FREEZING_CODES = (56, 57, 66, 67)   # коды погоды WMO: ледяная морось и ледяной дождь
SNOW_CODES = (71, 73, 75, 77, 85, 86)   # коды погоды WMO: снег, снежная крупа, снежные ливни

# Ранг явлений для сводки по городу: какое явление показать, если в точках оно разное.
# Коды WMO растут с силой только внутри группы (61 → 63 → 65), между группами — нет:
# MAX(75 «сильный снег», 80 «слабый ливень») = 80, MAX(73, 95) = 95 — снег пропадал из
# сводки (замечание РАГРАФ 2026-11-17). Порядок групп — по последствиям для служб города:
# ледяные осадки (гололёд) > снег > гроза > дождь и ливни > морось > туман > облачность.
WMO_RANK: dict[int, int] = {
    0: 0, 1: 1, 2: 2, 3: 3,                             # ясно … пасмурно
    45: 10, 48: 11,                                     # туман, изморозь
    51: 20, 53: 21, 55: 22,                             # морось
    61: 30, 80: 31, 63: 32, 81: 33, 65: 34, 82: 35,     # дождь и ливни — по силе
    95: 40, 96: 41, 99: 42,                             # гроза, гроза с градом
    77: 50, 71: 51, 85: 52, 73: 53, 86: 54, 75: 55,     # снег — по силе
    56: 60, 57: 61, 66: 62, 67: 63,                     # ледяная морось и ледяной дождь
}


def strongest_weathercode(codes) -> int | None:
    """Самое значимое явление среди точек — по рангу WMO_RANK, а не по наибольшему коду."""
    known = [int(c) for c in codes if c is not None]
    if not known:
        return None
    return max(known, key=lambda c: (WMO_RANK.get(c, -1), c))


# Нормы для текстов служащим — чтобы табло было аргументом в разговоре с мэрией.
# Источники и пометки [П]/[В] — docs/EKOLOGIYA-POGODA-ISSLEDOVANIE.md (исследование 2026-11-17).
# П. 3.4 ГОСТ Р 50597-2017 называет источник прогноза — дорожные метеостанции и организации
# Росгидромета; прогноз модели общего назначения (Open-Meteo) срок не запускает.
NORMA_GOLOLED = ("Норма: скользкость на улицах устранить за 5–6 ч с момента обнаружения, на тротуарах — "
                 "за 12–24 ч (ГОСТ Р 50597-2017, табл. 8.1, 8.4). Обнаружением считается и прогноз её "
                 "образования на ближайшие 4 ч с дорожных метеостанций или от Росгидромета (п. 3.4). "
                 "Прогноз СИМА — модель Open-Meteo, срок он не запускает: это повод запросить прогноз "
                 "Западно-Сибирского УГМС.")
NORMA_MOROZ = ("Расчётная температура отопления Новосибирска −37 °C (СП 131.13330.2018): по мере "
               "приближения к ней — режим повышенной готовности теплосетей, плановые отключения — "
               "не на дни мороза.")
NORMA_PDK = "Порог — среднесуточная ПДК PM2.5 по СанПиН 1.2.3685-21 (0,035 мг/м³)."
NORMA_NMU = "Основание: ст. 19 закона 96-ФЗ в ред. 548-ФЗ (с 01.03.2026), приказ Минприроды № 651."


def _ice_rules() -> dict:
    """Параметры гололёда из ecology_rules.yaml (risks.ice, версия 1.1)."""
    return _eco_rules().get("ice", {}) or {}


def ice_level(day: dict, next_day: dict | None, cfg: dict | None = None) -> str | None:
    """Гололёд по влаге, а не по одной температуре (ecology_rules 1.1, решение руководителя работы 2026-11-17).

    «glaze»    — ледяной дождь или морось в прогнозе на сутки («гололёд»);
    «refreeze» — днём дождь не меньше refreeze_rain_mm, а следующей ночью не выше
                 refreeze_night_temp_c («возможна гололедица»);
    None       — иначе: в сухую погоду около нуля оповещения нет.

    Для сводки по городу ледяной дождь берётся из freezing_any — «хотя бы в одной точке»;
    для одной точки — из weathercode. Код погоды по точкам не усредняют: среднее «мороси» (51)
    и «дождя» (61) — 56, ложный ледяной дождь. В сводке — самое значимое явление по рангу
    WMO_RANK (strongest_weathercode), а не наибольший код.
    """
    cfg = _ice_rules() if cfg is None else cfg
    codes = {int(c) for c in cfg.get("freezing_codes", FREEZING_CODES)}
    if "freezing_any" in day:
        glaze = bool(day.get("freezing_any"))
    else:
        glaze = day.get("weathercode") is not None and int(day["weathercode"]) in codes
    if glaze:
        return "glaze"
    rain = day.get("rain_mm")
    night = (next_day or {}).get("temp_min")
    if (rain is not None and night is not None
            and rain >= float(cfg.get("refreeze_rain_mm", 1.0))
            and night <= float(cfg.get("refreeze_night_temp_c", -1.0))):
        return "refreeze"
    return None


def _forecast_days(conn, district_filter: str | None, date_from: str, date_to: str,
                   include_from: bool = True) -> list[dict]:
    """Прогноз по суткам, сводка по точкам: средние температуры и осадки, наибольший ветер;
    freezing_any — ледяной дождь или морось хотя бы в одной точке."""
    codes = [int(c) for c in _ice_rules().get("freezing_codes", FREEZING_CODES)] or [-1]
    wheres = [f"forecast_date {'>=' if include_from else '>'} ?", "forecast_date <= ?"]
    params: list = [date_from, date_to]
    if district_filter:
        wheres.append("district ILIKE ?")
        params.append(f"%{district_filter.split()[0]}%")
    sql = f"""
        SELECT
            forecast_date,
            ROUND(AVG(temp_max), 1)      AS temp_max,
            ROUND(AVG(temp_min), 1)      AS temp_min,
            ROUND(MAX(wind_max), 1)      AS wind_max,
            ROUND(AVG(precipitation), 1) AS precipitation,
            ROUND(AVG(snowfall_cm), 1)   AS snowfall_cm,
            ROUND(AVG(rain_mm), 1)       AS rain_mm,
            MAX(CASE WHEN weathercode IN ({", ".join("?" * len(codes))}) THEN 1 ELSE 0 END) AS freezing_any,
            -- коды точек целиком: явление дня выбирается по рангу, а не средним (3 и 61 дают 34 —
            -- такого кода нет) и не наибольшим кодом (80 «ливень» выше 75 «сильный снег»)
            LIST(weathercode)            AS codes
        FROM eco_forecast
        WHERE {" AND ".join(wheres)}
        GROUP BY forecast_date
        ORDER BY forecast_date
    """
    cursor = conn.execute(sql, codes + params)
    cols = [d[0] for d in cursor.description]
    rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
    for r in rows:
        r["weathercode"] = strongest_weathercode(r.pop("codes") or [])
    return rows


def _ice_today(district_filter: str | None) -> tuple[str | None, dict | None, dict | None]:
    """Гололёд на сегодня: сутки прогноза и следующая ночь — по местному времени города."""
    try:
        from .city_config import get_utc_offset
        offset = int(get_utc_offset())
    except Exception:
        offset = 7
    today = (datetime.now(timezone.utc) + timedelta(hours=offset)).date()
    init_ecology_tables()
    conn = _get_conn()
    try:
        rows = _forecast_days(conn, district_filter, str(today), str(today + timedelta(days=1)))
    except Exception as e:
        log.warning("гололёд: прогноз недоступен (%s)", e)
        return None, None, None
    finally:
        conn.close()
    by_day = {str(r["forecast_date"])[:10]: r for r in rows}
    seg, zavtra = by_day.get(str(today)), by_day.get(str(today + timedelta(days=1)))
    return (ice_level(seg, zavtra) if seg else None), seg, zavtra

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
    rain_mm       DOUBLE,               -- дождь мм/сутки (Open-Meteo rain_sum) — для гололедицы
    weathercode   INTEGER,
    fetched_at    VARCHAR
)
"""

# Архив выданных прогнозов: строка на точку, дату прогноза и момент выпуска (fetched_at)
_DDL_FORECAST_ISSUES = """
CREATE TABLE IF NOT EXISTS eco_forecast_issues (
    station_id    VARCHAR,
    district      VARCHAR,
    forecast_date VARCHAR,
    issued_at     VARCHAR,              -- момент загрузки прогноза (fetched_at)
    temp_max      DOUBLE,
    temp_min      DOUBLE,
    wind_max      DOUBLE,
    precipitation DOUBLE,
    snowfall_cm   DOUBLE,
    rain_mm       DOUBLE,
    weathercode   INTEGER,
    PRIMARY KEY (station_id, forecast_date, issued_at)
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
        # Миграции: колонки, добавленные после создания таблицы
        for col in ("snowfall_cm", "rain_mm"):
            try:
                conn.execute(f"ALTER TABLE eco_forecast ADD COLUMN {col} DOUBLE")
            except Exception:
                pass  # колонка уже есть
        conn.execute(_DDL_FORECAST_ISSUES)
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
            last = conn.execute(
                "SELECT MAX_BY(measured_at, CAST(measured_at AS TIMESTAMPTZ)) FROM fact_measurements"
            ).fetchone()[0] or ""
            total = conn.execute("SELECT COUNT(*) FROM fact_measurements").fetchone()[0]
            # Покрытие районов — по последнему измерению КАЖДОЙ станции, а не по
            # глобальному максимуму времени: неполный снимок Open-Meteo иначе
            # срезает покрытие до числа станций, попавших именно в этот снимок.
            districts = conn.execute(
                """
                WITH latest AS (
                    SELECT station_id
                    FROM fact_measurements
                    WHERE CAST(measured_at AS TIMESTAMPTZ) >= NOW() - INTERVAL 24 HOUR
                    GROUP BY station_id
                )
                SELECT COUNT(DISTINCT s.district)
                FROM latest
                JOIN dim_stations s ON s.station_id = latest.station_id
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

    Если у последнего снимка отсутствует метео (Open-Meteo Forecast периодически
    отдаёт null), берём последнее непустое значение temp/wind/humidity по этой
    же станции за 24 часа — чтобы UI не пустовал.
    """
    init_ecology_tables()
    conn = _get_conn()
    try:
        # Последнее измерение берём ПО КАЖДОЙ СТАНЦИИ, а не по глобальному
        # максимуму времени. Причины:
        #  1. Open-Meteo периодически отдаёт неполный снимок — в него попадает
        #     часть станций. При сравнении с единственным MAX остальные районы
        #     просто исчезали из «качества воздуха сейчас» (наблюдали 5 из 11).
        #  2. measured_at хранится строкой со смещением, и строковый MAX
        #     при смешанных зонах врёт: '…04:53+00:00' лексикографически меньше
        #     '…11:51+07:00', хотя это 11:53 против 11:51 — то есть позже.
        #     Поэтому сравниваем как TIMESTAMPTZ.
        wheres = ["f.measured_at = latest.measured_at"]
        params: list = []
        if district_filter:
            wheres.append("s.district ILIKE ?")
            params.append(f"%{district_filter.split()[0]}%")
        where_sql = "WHERE " + " AND ".join(wheres)
        sql = f"""
            WITH latest AS (
                SELECT station_id,
                       MAX_BY(measured_at, CAST(measured_at AS TIMESTAMPTZ)) AS measured_at
                FROM fact_measurements
                WHERE CAST(measured_at AS TIMESTAMPTZ) >= NOW() - INTERVAL 24 HOUR
                GROUP BY station_id
            ),
            last_weather AS (
                SELECT station_id,
                       MAX_BY(temperature_c, measured_at) FILTER (WHERE temperature_c IS NOT NULL) AS temp_fb,
                       MAX_BY(wind_speed_ms, measured_at) FILTER (WHERE wind_speed_ms IS NOT NULL) AS wind_fb,
                       MAX_BY(humidity_pct,  measured_at) FILTER (WHERE humidity_pct  IS NOT NULL) AS hum_fb,
                       MAX_BY(measured_at,   measured_at) FILTER (WHERE temperature_c IS NOT NULL) AS weather_at
                FROM fact_measurements
                WHERE measured_at >= strftime(NOW() - INTERVAL 24 HOUR, '%Y-%m-%dT%H:%M:%S')
                GROUP BY station_id
            )
            SELECT
                s.district,
                s.address,
                ROUND(f.pm25, 1)          AS pm25,
                ROUND(f.pm10, 1)          AS pm10,
                ROUND(f.no2,  1)          AS no2,
                f.aqi,
                ROUND(COALESCE(f.temperature_c, lw.temp_fb), 1) AS temperature_c,
                ROUND(COALESCE(f.wind_speed_ms, lw.wind_fb), 1) AS wind_speed_ms,
                ROUND(COALESCE(f.humidity_pct,  lw.hum_fb),  0) AS humidity_pct,
                CASE WHEN f.temperature_c IS NULL AND lw.temp_fb IS NOT NULL
                     THEN lw.weather_at ELSE NULL END AS weather_fallback_at,
                f.source,
                f.measured_at
            FROM fact_measurements f
            JOIN latest ON latest.station_id = f.station_id
            JOIN dim_stations s ON f.station_id = s.station_id
            LEFT JOIN last_weather lw ON lw.station_id = f.station_id
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
    """Записи за сегодня, где PM2.5 > 35 мкг/м³ (среднесуточная ПДК по СанПиН 1.2.3685-21, ТЗ §5).

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
      pdk          — PM2.5 > 35 мкг/м³ (превышение среднесуточной ПДК)
      ice          — гололёд по влаге (ecology_rules 1.1): ледяной дождь или морось сегодня —
                     «гололёд»; дождь днём и мороз следующей ночью — «возможна гололедица»
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
    # воздух только из модели (Open-Meteo = CAMS), без датчиков — честная оговорка в тексте
    _tolko_model = all(str(r.get("source") or "").startswith("open-meteo") for r in rows)
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
    _shock    = _r.get("temp_shock",   {})
    _cold     = _r.get("extreme_cold", {})

    smog_wind_thr    = float(_smog.get("wind_threshold_ms",         1.5))
    smog_warn_pm25   = float(_smog.get("pm25_warning_threshold",   20.0))
    smog_crit_pm25   = float(_smog.get("pm25_critical_threshold",  35.0))
    pdk_thr          = float(_pdk.get("threshold",                 35.0))
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
                "Режим НМУ объявляет Западно-Сибирское УГМС, выбросы предприятия снижают по своим планам. "
                "Мэрии — организовать мероприятия по областному порядку: оповестить жителей и учреждения, "
                "перенести пылящие работы, усилить контроль сжигания мусора. " + NORMA_NMU
            ),
        })

    # ── Превышение среднесуточной ПДК PM2.5 ────────────────────────────────────────
    if avg_pm25 is not None and avg_pm25 > pdk_thr:
        risks.append({
            "id": "pdk",
            "scenario": "Превышение ПДК PM2.5",
            "severity": "critical",
            "icon": "☢️",
            "title": f"PM2.5 = {avg_pm25} мкг/м³ — норма превышена в {round(avg_pm25 / pdk_thr, 1)}×",
            "metrics": f"ПДК среднесуточная: {pdk_thr} мкг/м³ · Текущее: {avg_pm25} мкг/м³",
            "citizen": (
                "Ограничьте время на улице. "
                "Носите маску класса FFP2 / N95. "
                "Не проветривайте помещение, закройте окна."
            ),
            "official": (
                "Задействовать систему экстренного оповещения населения. "
                "Рекомендовать отмену открытых массовых мероприятий. " + NORMA_PDK
                + (" Данные СИМА — расчёт модели CAMS, а не замер: перед мерами сверить с постами УГМС."
                   if _tolko_model else "")
            ),
        })

    # ── Сценарий А: гололёд — по влаге, а не по одной температуре (ecology_rules 1.1) ──
    # Было: «средняя от −3 до +2 °C» — в ноябре 2026 горело 14 суток из 16 при сухой погоде.
    # Первыми обмерзают мосты, эстакады и путепроводы: под ними нет тёплой подушки грунта.
    ice_lvl, seg, zavtra = _ice_today(district_filter)
    _t = lambda x: f"{x:+.0f}" if x is not None else "—"
    if ice_lvl == "glaze":
        risks.append({
            "id": "ice",
            "scenario": "Гололёд",
            "severity": "critical",
            "icon": "🧊",
            "title": "Ледяной дождь — гололёд",
            "metrics": f"Осадки {seg.get('precipitation') or 0} мм · {_t(seg.get('temp_min'))}…{_t(seg.get('temp_max'))} °C",
            "citizen": (
                "Ледяной дождь: дороги и тротуары покрываются коркой льда. Раньше всего — "
                "на мостах, эстакадах и путепроводах: под ними нет тёплой подушки грунта. "
                "Если можно, отложите поездку; пешеходам — обходить спуски и лестницы."
            ),
            "official": (
                "Ледяной дождь по прогнозу на сегодня. В первую очередь обработать мосты через Обь, "
                "эстакады и путепроводы — они обмерзают раньше дорог на грунте; затем спуски, "
                "остановки, подходы к больницам и школам. Оповестить жителей. " + NORMA_GOLOLED
            ),
        })
    elif ice_lvl == "refreeze":
        risks.append({
            "id": "ice",
            "scenario": "Возможна гололедица",
            "severity": "warning",
            "icon": "🧊",
            "title": "Дождь, а ночью мороз — возможна гололедица",
            "metrics": f"Дождь {seg.get('rain_mm')} мм · ночью до {_t((zavtra or {}).get('temp_min'))} °C",
            "citizen": (
                "Днём дождь, ночью мороз: мокрое покрытие замёрзнет. Раньше всего — на мостах, "
                "эстакадах и путепроводах: под ними нет тёплой подушки грунта. Утром заложите "
                "время на дорогу, на спусках — осторожно."
            ),
            "official": (
                "К утру обработать мосты через Обь, эстакады и путепроводы — они обмерзают первыми; "
                "затем спуски, остановки, подходы к больницам и школам. " + NORMA_GOLOLED
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
                "Контролировать теплоснабжение социальных объектов (школы, больницы, дома престарелых). "
                + NORMA_MOROZ
            ),
        })

    return risks


def seed_history_placeholder(
    days: int = 20,
    temp_c: float = -10.0,
) -> int:
    """Заполняет ecology_daily_archive историческими данными-заглушками.

    ⚠️ ИЗ АВТОЗАПУСКА УБРАНА (см. api.py::_seed_ecology_history). Оставлена
    для ручного вызова, но пользоваться ей не стоит без крайней нужды:

    - строки физически недостоверны (одинаковые во всех районах 12.0 мкг/м³
      и −10 °C) и в летние сутки показывают зимнюю погоду;
    - ON CONFLICT DO NOTHING занимает первичный ключ (day, district), после
      чего импорт НАСТОЯЩИХ измерений за те же сутки молча отбрасывается —
      так уже терялись данные при восстановлении из бэкапа;
    - аналитика их всё равно игнорирует (snapshots = 0), а на графике они
      рисуются как «нет данных», так что косметической пользы больше нет.

    Убрать созданное можно через purge_placeholder_rows().

    Вставляет строки только для тех (day, district), которых ещё нет в архиве
    (ON CONFLICT DO NOTHING). Реальные данные от Open-Meteo автоматически
    перезапишут заглушки при очередном обновлении (ON CONFLICT DO UPDATE).

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


def purge_placeholder_rows() -> int:
    """Удаляет из архива строки-заглушки и возвращает их число.

    Маркер — snapshots = 0: измерений за сутки не было ни одного, значит
    строку записала seed_history_placeholder, а не сборщик. Заглушки несут
    одинаковые для всех районов 12.0 мкг/м³ и −10 °C, то есть в летние сутки
    показывают зимнюю погоду; в аналитику они не попадают, но раздувают
    счётчик «дней с данными» и на графике выглядят как наблюдение.

    Отдельная беда: заглушка занимает первичный ключ (day, district), и
    последующий импорт настоящих измерений за этот день молча отбрасывается —
    так уже терялись данные при восстановлении из бэкапа.
    """
    init_ecology_tables()
    conn = _get_conn()
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM ecology_daily_archive WHERE snapshots = 0"
        ).fetchone()[0]
        if n:
            conn.execute("DELETE FROM ecology_daily_archive WHERE snapshots = 0")
            log.info("purge_placeholder_rows: удалено %d строк-заглушек", n)
        return int(n)
    except Exception as e:
        log.error("purge_placeholder_rows: %s", e)
        return 0
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
                         temp_max, temp_min, wind_max, precipitation, snowfall_cm, rain_mm,
                         weathercode, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        temp_max      = excluded.temp_max,
                        temp_min      = excluded.temp_min,
                        wind_max      = excluded.wind_max,
                        precipitation = excluded.precipitation,
                        snowfall_cm   = excluded.snowfall_cm,
                        rain_mm       = excluded.rain_mm,
                        weathercode   = excluded.weathercode,
                        fetched_at    = excluded.fetched_at
                    """,
                    [
                        r["id"], r["station_id"], r.get("district", ""),
                        r["forecast_date"],
                        r.get("temp_max"), r.get("temp_min"),
                        r.get("wind_max"), r.get("precipitation"),
                        r.get("snowfall_cm"), r.get("rain_mm"), r.get("weathercode"), r["fetched_at"],
                    ],
                )
                count += 1
            except Exception as e:
                log.error(f"Forecast upsert error {r.get('id')}: {e}")
        log.info(f"Forecast upsert: {count} записей")
        _archive_forecast_issue(conn, records)
        return count
    finally:
        conn.close()


# ── Снегопад по районам на 1–3 суток (пилот 2026-11-18, docs/SNEGOPAD-RAZBOR.md) ──────────
# Ретропроверка прогноза «снег от 2 см за сутки» (фаза С.1): прошлые прогнозы Open-Meteo
# (best_match) против реанализа ERA5 по точке центра города, октябрь–апрель. Числа
# пересчитываются из docs/snegopad-2026/snow_retro_daily.csv — сторож в tests/test_snow_outlook.py.
SNOW_RETRO: dict[str, Any] = {
    "period": "01.2024–11.2026, октябрь–апрель",
    "truth": "ERA5",
    "threshold_cm": 2.0,
    "days": 569,
    "events": 60,
    "lead1": {"named": 0.78, "came_true": 0.78},
    "lead2": {"named": 0.72, "came_true": 0.75},
    "lead3": {"named": 0.65, "came_true": 0.74},
    "autumn_2026": {"forecasts": 7, "came_true": 2},
    "command": "python3 -I scripts/snow_retro/snow_retro.py score docs/snegopad-2026/snow_retro_daily.csv",
}
SNOW_LEVELS_DEFAULT = [{"level": 1, "min_cm": 2.0, "label": "Снег от 2 см"},
                       {"level": 2, "min_cm": 5.0, "label": "Снегопад от 5 см"},
                       {"level": 3, "min_cm": 15.0, "label": "Сильный снегопад от 15 см"}]


def snow_level(cm: float | None, levels: list[dict]) -> int:
    """Уровень снегопада за сутки: наибольший уровень, чей порог достигнут; 0 — ниже всех."""
    if cm is None:
        return 0
    return max([int(lv["level"]) for lv in levels if cm >= float(lv["min_cm"])], default=0)


def query_snow_outlook(district_filter: str | None = None) -> dict[str, Any]:
    """Снегопад по районам на horizon_days суток вперёд (с завтрашних, по местному времени).

    Мэру — районы × сутки и самый снежный день; району — его сутки. Действия ДЭУ, тексты УК и
    жителям — по каждому уровню, который встречается в горизонте (регламент ecology_rules,
    snow_outlook). Если в районе несколько точек — берётся наибольший снег (с запасом).
    """
    cfg = (_rules.get("ecology_rules") or {}).get("snow_outlook") or {}
    levels = sorted(cfg.get("levels") or SNOW_LEVELS_DEFAULT, key=lambda lv: float(lv["min_cm"]))
    horizon = int(cfg.get("horizon_days", 3))
    exclude = {str(x).lower() for x in cfg.get("exclude_districts", ["Кольцово"])}
    if district_filter and district_filter.lower().startswith("академ"):
        district_filter = "Советский"            # Академгородок — в Советском районе
    try:
        from .city_config import get_utc_offset
        offset = int(get_utc_offset())
    except Exception:
        offset = 7
    today = (datetime.now(timezone.utc) + timedelta(hours=offset)).date()
    days = [str(today + timedelta(days=k)) for k in range(1, horizon + 1)]

    init_ecology_tables()
    conn = _get_conn()
    try:
        wheres, params = ["forecast_date >= ?", "forecast_date <= ?"], [days[0], days[-1]]
        if district_filter:
            wheres.append("district ILIKE ?")
            params.append(f"%{district_filter.split()[0]}%")
        rows = conn.execute(
            f"SELECT district, forecast_date, snowfall_cm, temp_min, temp_max, fetched_at "
            f"FROM eco_forecast WHERE {' AND '.join(wheres)}", params).fetchall()
    finally:
        conn.close()

    by: dict[str, dict[str, dict]] = {}
    issued_at = None
    for dist, day, sf, tmin, tmax, fetched in rows:
        if not dist or str(dist).lower() in exclude:
            continue
        day = str(day)[:10]
        sf = round(float(sf), 1) if sf is not None else None
        cur = by.setdefault(dist, {}).get(day)
        if cur is None or (sf or 0) > (cur["snow_cm"] or 0):
            by[dist][day] = {"day": day, "snow_cm": sf, "t_min": tmin, "t_max": tmax}
        if fetched and (issued_at is None or str(fetched) > issued_at):
            issued_at = str(fetched)

    districts = []
    for dist in sorted(by):
        ds = [dict(by[dist].get(day) or {"day": day, "snow_cm": None, "t_min": None, "t_max": None})
              for day in days]
        for x in ds:
            x["level"] = snow_level(x["snow_cm"], levels)
        districts.append({"district": dist, "days": ds,
                          "total_cm": round(sum(x["snow_cm"] or 0 for x in ds), 1),
                          "max_level": max(x["level"] for x in ds)})

    cells = [(d["district"], x) for d in districts for x in d["days"]]
    city = []
    for day in days:
        here = sorted(((dist, x) for dist, x in cells if x["day"] == day and x["level"] >= 1),
                      key=lambda t: -(t[1]["snow_cm"] or 0))
        city.append({"day": day,
                     "max_cm": max([x["snow_cm"] or 0 for dist, x in cells if x["day"] == day], default=0),
                     "districts": [dist for dist, _ in here]})
    snowy = [(dist, x) for dist, x in cells if x["level"] >= 1]
    worst = None
    if snowy:
        dist, x = max(snowy, key=lambda t: (t[1]["snow_cm"] or 0, -days.index(t[1]["day"])))
        worst = {"district": dist, "day": x["day"], "snow_cm": x["snow_cm"], "level": x["level"]}
    actions = []
    for lv in sorted(levels, key=lambda lv: -int(lv["level"])):
        where = [{"district": dist, "day": x["day"], "snow_cm": x["snow_cm"]}
                 for dist, x in snowy if x["level"] == int(lv["level"])]
        if where:
            actions.append({"level": int(lv["level"]), "label": lv.get("label", ""), "min_cm": float(lv["min_cm"]),
                            "deu": lv.get("deu", ""), "uk": lv.get("uk", ""), "citizens": lv.get("citizens", ""),
                            "where": where})
    return {
        "operation": "SNOW_OUTLOOK",
        "district": district_filter or None,
        "error": None if rows else "прогноз погоды ещё не загружен",
        "today": str(today),
        "issued_at": issued_at,
        "days": [{"day": day} for day in days],
        "levels": [{"level": int(lv["level"]), "min_cm": float(lv["min_cm"]), "label": lv.get("label", "")}
                   for lv in levels],
        "districts": districts,
        "city": city,
        "worst": worst,
        "max_level": max([d["max_level"] for d in districts], default=0),
        "actions": actions,
        "source_note": cfg.get("source_note") or "Прогноз — модель Open-Meteo, а не прогноз УГМС.",
        "retro": SNOW_RETRO,
    }


def _archive_forecast_issue(conn, records: list[dict]) -> None:
    """Архив выданных прогнозов: eco_forecast перезаписывается, а выпуск остаётся.

    Выпуск — момент загрузки (fetched_at). Без архива нечем проверить свои же оповещения
    (исследование погоды, рекомендация 3; разбор снегопада, фаза С.4). Сбой архива не
    мешает прогнозу.
    """
    cols = ("station_id", "district", "forecast_date", "fetched_at", "temp_max", "temp_min", "wind_max",
            "precipitation", "snowfall_cm", "rain_mm", "weathercode")
    rows = [[r.get(c) if c != "district" else r.get("district", "") for c in cols]
            for r in records if r.get("station_id") and r.get("forecast_date") and r.get("fetched_at")]
    try:
        for i in range(0, len(rows), 200):
            part = rows[i:i + 200]
            conn.execute(
                "INSERT INTO eco_forecast_issues VALUES "
                + ", ".join(["(" + ", ".join(["?"] * len(cols)) + ")"] * len(part)) + " ON CONFLICT DO NOTHING",
                [v for row in part for v in row],
            )
    except Exception as e:
        log.warning("архив выпусков погоды не записан: %s", e)


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


def _local_today(now: datetime | None = None):
    """«Сегодня» по местному времени города.

    Прогноз Open-Meteo хранится по местным суткам (timezone=Asia/Novosibirsk). С 00:00 до 07:00
    по Новосибирску дата по UTC ещё вчерашняя, и окно «после сегодня» по UTC захватывало бы
    сегодняшние местные сутки (сверка с РАГРАФ 18.11, С4).
    """
    try:
        from .city_config import get_utc_offset
        offset = int(get_utc_offset())
    except Exception:
        offset = 7
    return ((now or datetime.now(timezone.utc)) + timedelta(hours=offset)).date()


def query_forecast(district_filter: str | None = None, days: int = 7,
                   include_today: bool = False) -> list[dict]:
    """Возвращает 7-дневный прогноз погоды агрегированный по дням.

    Если district_filter задан — только данные по этому району.
    Иначе — среднее по всем районам (агрегированное по городу).

    Окно — по местным суткам: сутки 1…days после сегодняшних; include_today — и сегодняшние
    (days=0, include_today=True — только сегодня).

    Возвращает список dict с полями:
      forecast_date, day_name, temp_max, temp_min, wind_max,
      precipitation, snowfall_cm, weathercode, weather_icon, weather_desc,
      ice_risk, cold_risk, snow_risk, snow_impact
    """
    init_ecology_tables()
    conn = _get_conn()
    try:
        day0 = _local_today()
        today = str(day0)
        cutoff = str(day0 + timedelta(days=days))

        rows = _forecast_days(conn, district_filter, today, cutoff, include_from=include_today)

        # Обогащаем каждый день: иконка, описание, флаги рисков
        ru_days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        ru_months = ["янв", "фев", "мар", "апр", "май", "июн",
                     "июл", "авг", "сен", "окт", "ноя", "дек"]
        from datetime import date as _date
        for i, r in enumerate(rows):
            dt = _date.fromisoformat(r["forecast_date"])
            r["day_name"] = f"{ru_days[dt.weekday()]} {dt.day} {ru_months[dt.month - 1]}"
            icon, desc = _wmo_label(r["weathercode"])
            r["weather_icon"] = icon
            r["weather_desc"] = desc
            t_max = r["temp_max"]
            t_min = r["temp_min"]
            # Прогнозные флаги рисков из ecology_rules.yaml
            _fc    = _rules.get("ecology_rules").get("forecast", {})
            _cold_thr  = float(_fc.get("cold_risk", {}).get("threshold", -20.0))
            _snow_codes = {int(c) for c in (_fc.get("snow_risk") or {}).get("snow_codes", SNOW_CODES)}

            # Гололёд — по влаге (ecology_rules 1.1): ледяной дождь в сутки или дождь днём и
            # мороз следующей ночью; для последних суток прогноза ночь неизвестна
            lvl = ice_level(r, rows[i + 1] if i + 1 < len(rows) else None)
            r["ice_level"] = lvl
            r["ice_glaze"] = lvl == "glaze"
            r["ice_refreeze"] = lvl == "refreeze"
            r["ice_risk"] = lvl is not None
            # Риск сильного мороза
            r["cold_risk"] = bool(t_min is not None and t_min < _cold_thr)
            # Риск снегопада: осадки и код снега (ecology_rules 1.2). Прежний диапазон 70–86
            # включал ливневый дождь 80–82 и горел летом (замечание РАГРАФ 2026-11-17)
            r["snow_risk"] = bool(r.get("precipitation") and r.get("weathercode") in _snow_codes)

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


def life_index_inputs(district_filter: str | None = None) -> dict:
    """Входы индексов жизни из прогноза: гололёд на сегодня, ночной минимум, снегопад на 3 суток.

    Одни и те же для /ecology/status, /life-indices и ответа «качество воздуха» (/ask), чтобы
    табло и сервер считали по одним величинам. Нет прогноза — нет величины (None, ⊥), а не 0:
    индекс тогда помечается «неполные данные» (сверка с РАГРАФ 18.11, life_indices_rules 1.3).

    night_min — минимум следующих суток прогноза: так же «следующую ночь» берёт правило
    гололёда (ice_level). Гололёд и ночной минимум — по району, если он задан; снегопад —
    наибольший по городу за 3 суток, как было на табло.
    """
    out: dict = {"ice_level": None, "night_min": None, "snow_impact": None}
    try:
        ice, _seg, zavtra = _ice_today(district_filter)
        out["ice_level"] = ice
        if zavtra and zavtra.get("temp_min") is not None:
            out["night_min"] = float(zavtra["temp_min"])
    except Exception as e:
        log.warning("индексы жизни: гололёд и ночной минимум не оценены (%s)", e)
    try:
        vals = [float(r["snow_impact"]) for r in query_forecast(days=3) if r.get("snow_impact") is not None]
        if vals:
            out["snow_impact"] = max(vals)
    except Exception as e:
        log.warning("индексы жизни: снегопад не оценён (%s)", e)
    return out


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


def _percentile(values: list[float], q: float) -> float:
    """Перцентиль по линейной интерполяции. q в долях: 0.95 → P95."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = q * (len(ordered) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def query_district_ecology_rating(days: int = 30) -> list[dict]:
    """Рейтинг районов по экологии за N дней.

    Возвращает для каждого района (отсортировано: лучший → худший):
      - district
      - aqi_avg, aqi_max, aqi_min — характеристики AQI за период
      - pm25_avg, pm25_max
      - days_total — кол-во дней с данными
      - days_clean — дней с AQI <= 35 (хорошее качество)
      - days_exceed — дней с AQI > 50 (плохое)
      - score (0-10) — комплексный балл качества воздуха
      - grade (A-F) — оценка
      - temp_avg, wind_avg — для контекста

    Логика score (10 = идеально, 0 = критично):
      - Стартовый score = 10
      - Штраф за средний AQI:
          AQI <= 25 → 0
          AQI <= 35 → -1
          AQI <= 50 → -3
          AQI <= 70 → -5
          AQI >  70 → -7
      - Штраф за дни с превышениями (AQI > 50): -0.2 за каждый
      - Бонус за чистые дни (AQI <= 35): +0.1 за каждый
      - Штраф за PM2.5 пик: PM2.5 > 35 мкг/м³ — -1; > 55 — -2
    """
    init_ecology_tables()
    rows = query_history(days=days)
    if not rows:
        return []

    # Группируем по району
    by_dist: dict[str, dict] = {}
    for r in rows:
        dist = r.get("район")
        if not dist:
            continue
        # snapshots=0 — строка-заглушка от seed_history_placeholder (одинаковые
        # 12.0 мкг/м³ и −10 °C во всех районах). В рейтинг её пускать нельзя:
        # она усредняет реальные различия и в летние сутки даёт зимнюю погоду.
        if not (r.get("снимков") or 0):
            continue
        if dist not in by_dist:
            by_dist[dist] = {
                "district": dist,
                "_aqi_vals": [], "_pm25_vals": [], "_pm25_max_vals": [],
                "_temp_vals": [], "_wind_vals": [],
                "days_total": 0, "days_clean": 0, "days_exceed": 0,
            }
        d = by_dist[dist]
        aqi = r.get("aqi_ср")
        pm25 = r.get("pm25_ср")
        pm25_max = r.get("pm25_макс")
        temp = r.get("темп_ср")
        wind = r.get("ветер_ср")

        d["days_total"] += 1
        if aqi is not None:
            d["_aqi_vals"].append(float(aqi))
            if float(aqi) <= 35:
                d["days_clean"] += 1
            if float(aqi) > 50:
                d["days_exceed"] += 1
        if pm25 is not None:
            d["_pm25_vals"].append(float(pm25))
        if pm25_max is not None:
            d["_pm25_max_vals"].append(float(pm25_max))
        if temp is not None:
            d["_temp_vals"].append(float(temp))
        if wind is not None:
            d["_wind_vals"].append(float(wind))

    # Расчёт метрик и грейдов
    result = []
    for dist, d in by_dist.items():
        aqi_vals = d["_aqi_vals"]
        pm25_vals = d["_pm25_vals"]
        pm25_max_vals = d["_pm25_max_vals"]

        aqi_avg = round(sum(aqi_vals) / len(aqi_vals), 1) if aqi_vals else 0
        aqi_max = round(max(aqi_vals), 1) if aqi_vals else 0
        aqi_min = round(min(aqi_vals), 1) if aqi_vals else 0
        pm25_avg = round(sum(pm25_vals) / len(pm25_vals), 1) if pm25_vals else 0
        pm25_max = round(max(pm25_max_vals or pm25_vals or [0]), 1)
        temp_avg = round(sum(d["_temp_vals"]) / len(d["_temp_vals"]), 1) if d["_temp_vals"] else None
        wind_avg = round(sum(d["_wind_vals"]) / len(d["_wind_vals"]), 1) if d["_wind_vals"] else None

        # Пики за период — для score берём 95-й перцентиль, а не абсолютный
        # максимум. Максимум растёт с длиной окна просто потому, что редкое
        # событие успевает попасть в выборку: на 30 днях AQI-макс ≈ 55, на 150
        # тех же данных ≈ 88, и рейтинг «ухудшался» от одного лишь расширения
        # периода. Перцентиль от длины окна почти не зависит. В таблицу при
        # этом отдаём настоящий максимум — он информативен для читателя.
        aqi_p95 = _percentile(aqi_vals, 0.95) if aqi_vals else 0
        pm25_p95 = _percentile(pm25_max_vals or pm25_vals, 0.95) if (pm25_max_vals or pm25_vals) else 0

        # Score — 4 компонента по 2.5 балла каждый = 10 макс.
        # Узкие диапазоны для дифференциации близких значений.

        # 1. AQI среднее: 20→2.5, 50→0 (диапазон 30 ед. для дифференциации)
        s_aqi_avg = max(0, min(2.5, 2.5 * (50 - aqi_avg) / 30)) if aqi_avg else 2.5

        # 2. AQI пик за период (P95): 30→2.5, 75→0
        s_aqi_max = max(0, min(2.5, 2.5 * (75 - aqi_p95) / 45)) if aqi_p95 else 2.5

        # 3. PM2.5 пик (P95): 10→2.5, 45→0
        s_pm25 = max(0, min(2.5, 2.5 * (45 - pm25_p95) / 35)) if pm25_p95 else 2.5

        # 4. Доля чистых дней (AQI≤35) минус превышения: 100% → 2.5
        days_total = max(d["days_total"], 1)
        clean_pct = d["days_clean"] / days_total
        exceed_pct = d["days_exceed"] / days_total
        s_days = 2.5 * (clean_pct - exceed_pct * 0.5)
        s_days = max(0, min(2.5, s_days))

        score = round(s_aqi_avg + s_aqi_max + s_pm25 + s_days, 2)
        score = max(0.0, min(10.0, score))

        # Грейд
        if score >= 8:
            grade = "A"
        elif score >= 6.5:
            grade = "B"
        elif score >= 5:
            grade = "C"
        elif score >= 3:
            grade = "D"
        else:
            grade = "F"

        result.append({
            "district": dist,
            "score": score,
            "grade": grade,
            "aqi_avg": aqi_avg,
            "aqi_max": aqi_max,
            "aqi_min": aqi_min,
            "aqi_p95": round(aqi_p95, 1),
            "pm25_avg": pm25_avg,
            "pm25_max": pm25_max,
            "pm25_p95": round(pm25_p95, 1),
            "temp_avg": temp_avg,
            "wind_avg": wind_avg,
            "days_total": d["days_total"],
            "days_clean": d["days_clean"],
            "days_exceed": d["days_exceed"],
        })

    # Сортировка: лучший AQI → худший (меньше AQI = лучше)
    result.sort(key=lambda r: (r["aqi_avg"], -r["score"]))
    return result


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
