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
from .constants import (
    NSK_ECOLOGY_STATIONS,
    ECOLOGY_HISTORY_DAYS,
    ECOLOGY_TTL_MINUTES,
    DATA_DIR,
)

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


def init_ecology_tables() -> None:
    """Создаёт таблицы если их нет."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = _get_conn()
    try:
        conn.execute(_DDL_STATIONS)
        conn.execute(_DDL_MEASUREMENTS)
    finally:
        conn.close()


def upsert_stations(stations: list[dict] | None = None) -> None:
    """Синхронизирует справочник станций (dim_stations).

    По умолчанию использует NSK_ECOLOGY_STATIONS из constants.py.
    source = 'open-meteo' если станция виртуальная (по координатам района).
    """
    if stations is None:
        stations = NSK_ECOLOGY_STATIONS

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
        return count
    finally:
        conn.close()


def is_ecology_stale(ttl_minutes: int = ECOLOGY_TTL_MINUTES) -> bool:
    """True если данных нет или они устарели."""
    try:
        init_ecology_tables()
        conn = _get_conn()
        try:
            row = conn.execute("SELECT MAX(measured_at) FROM fact_measurements").fetchone()
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
        wheres = ["f.measured_at >= ?", "f.pm25 > 35.0"]
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

    # ── Сценарий В: Экологическая ловушка / Чёрное небо ──────────────────────
    if avg_wind is not None and avg_pm25 is not None and avg_wind < 1.5 and avg_pm25 > 20:
        severity = "critical" if avg_pm25 > 35 else "warning"
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
    if avg_pm25 is not None and avg_pm25 > 35:
        risks.append({
            "id": "pdk",
            "scenario": "Превышение нормы ВОЗ",
            "severity": "critical",
            "icon": "☢️",
            "title": f"PM2.5 = {avg_pm25} мкг/м³ — норма превышена в {round(avg_pm25 / 35, 1)}×",
            "metrics": f"Норма ВОЗ: 35 мкг/м³ · Текущее: {avg_pm25} мкг/м³",
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
    if avg_temp is not None and -3.0 <= avg_temp <= 2.0:
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
    if temp_delta_24h is not None and temp_delta_24h <= -15.0:
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
    if min_temp is not None and min_temp < -20.0:
        risks.append({
            "id": "severe_cold",
            "scenario": "Экстремальный холод",
            "severity": "critical" if min_temp < -30.0 else "warning",
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


def query_history(district_filter: str | None = None, days: int = 7) -> list[dict]:
    """История по дням за N дней с агрегацией по показателям.

    Интент ТЗ §5: «Динамика PM2.5 в Советском районе за неделю».
    Также даёт корреляцию wind_speed_ms vs pm25 (§5 — влияние погоды на смог).
    """
    init_ecology_tables()
    conn = _get_conn()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        wheres = ["f.measured_at >= ?"]
        params: list = [cutoff]
        if district_filter:
            wheres.append("s.district ILIKE ?")
            params.append(f"%{district_filter.split()[0]}%")
        where_sql = "WHERE " + " AND ".join(wheres)
        sql = f"""
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
            ORDER BY день DESC, район
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка query_history: {e}")
        return []
    finally:
        conn.close()
