"""ETL-адаптер для данных о качестве воздуха и погоде в Новосибирске.

Источники:
1. Open-Meteo Air Quality API (бесплатно, без ключа) — PM2.5, PM10, AQI
2. Open-Meteo Weather API  (бесплатно, без ключа) — температура, ветер, давление
3. CityAir REST API (опционально, требует CITYAIR_API_KEY в .env) — телеметрия датчиков

Запускается по расписанию (cron / CLI: bot ecology update).
Частота: Open-Meteo — каждые 15 мин, CityAir — каждые 15 мин.
"""

import logging
import logging.handlers
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests

from .constants import (
    NSK_ECOLOGY_STATIONS,
    ECOLOGY_LOG_MAX_BYTES,
    LOGS_DIR,
    SCRAPER_TIMEOUT,
)

# ── Логирование с ротацией 10 МБ (ТЗ §6) ─────────────────────────────────────
LOGS_DIR.mkdir(parents=True, exist_ok=True)
_log_handler = logging.handlers.RotatingFileHandler(
    LOGS_DIR / "ecology.log",
    maxBytes=ECOLOGY_LOG_MAX_BYTES,
    backupCount=3,
    encoding="utf-8",
)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(_log_handler)
    log.setLevel(logging.INFO)

# ── Open-Meteo URLs (публичное API без ключа) ─────────────────────────────────
_OPENMETEO_AQ_URL     = "https://air-quality-api.open-meteo.com/v1/air-quality"
_OPENMETEO_WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

# Стандарт ВОЗ: PM2.5 > 15 мкг/м³ (среднесуточный) / > 35 мкг/м³ (24 ч WHO AQI alert)
PM25_WHO_THRESHOLD = 35.0

_RETRY_DELAYS = [2, 5, 10]  # секунды между повторными попытками


def _get_with_retry(url: str, params: dict, timeout: int = SCRAPER_TIMEOUT) -> dict | None:
    """GET-запрос с Retry-policy (ТЗ §3.1). Возвращает JSON или None при ошибке."""
    last_exc: Exception | None = None
    for attempt, delay in enumerate([0] + _RETRY_DELAYS, start=1):
        if delay:
            time.sleep(delay)
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout as e:
            log.warning(f"Таймаут ({attempt}/4): {url} — {e}")
            last_exc = e
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            if status and int(status) < 500:
                log.error(f"Клиентская ошибка {status}: {url} — запрос не повторяем")
                return None
            log.warning(f"Серверная ошибка {status} ({attempt}/4): {url}")
            last_exc = e
        except Exception as e:
            log.error(f"Ошибка запроса ({attempt}/4): {url} — {e}")
            last_exc = e
    log.error(f"Все попытки исчерпаны для {url}: {last_exc}")
    return None


def _fetch_openmeteo_air_quality(station: dict) -> dict | None:
    """Запрашивает PM2.5, PM10, AQI с Open-Meteo Air Quality API."""
    data = _get_with_retry(_OPENMETEO_AQ_URL, {
        "latitude":  station["latitude"],
        "longitude": station["longitude"],
        "current":   "pm10,pm2_5,european_aqi,nitrogen_dioxide",
        "timezone":  "Asia/Novosibirsk",
    })
    if not data or "current" not in data:
        return None
    cur = data["current"]
    return {
        "pm25": cur.get("pm2_5"),
        "pm10": cur.get("pm10"),
        "no2":  cur.get("nitrogen_dioxide"),
        "aqi":  cur.get("european_aqi"),
        "source": "open-meteo-aq",
    }


def _fetch_openmeteo_weather(station: dict) -> dict | None:
    """Запрашивает температуру, ветер, давление с Open-Meteo Forecast API."""
    data = _get_with_retry(_OPENMETEO_WEATHER_URL, {
        "latitude":        station["latitude"],
        "longitude":       station["longitude"],
        "current":         "temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,surface_pressure",
        "wind_speed_unit": "ms",
        "timezone":        "Asia/Novosibirsk",
    })
    if not data or "current" not in data:
        return None
    cur = data["current"]
    return {
        "temperature_c":     cur.get("temperature_2m"),
        "wind_speed_ms":     cur.get("wind_speed_10m"),
        "wind_direction_deg": cur.get("wind_direction_10m"),
        "humidity_pct":      cur.get("relative_humidity_2m"),
        "pressure_hpa":      cur.get("surface_pressure"),
        "source": "open-meteo-weather",
    }


def _fetch_cityair(station: dict) -> dict | None:
    """Запрашивает данные с CityAir API (если настроен ключ в .env).

    Требует переменных окружения:
      CITYAIR_API_KEY — ключ доступа к API
      CITYAIR_API_URL — базовый URL эндпоинта (уточните у поставщика)

    Возвращает None если ключ не задан или запрос неудачен.
    """
    api_key = os.environ.get("CITYAIR_API_KEY", "").strip()
    api_url = os.environ.get("CITYAIR_API_URL", "").strip()
    if not api_key or not api_url:
        return None

    data = _get_with_retry(api_url, {
        "lat": station["latitude"],
        "lon": station["longitude"],
        "apikey": api_key,
    })
    if not data:
        return None

    # Нормализация ответа CityAir → единый формат
    # Адаптируйте mapping под реальную структуру ответа CityAir
    try:
        return {
            "pm25": data.get("pm25") or data.get("PM2_5") or data.get("pm2_5"),
            "pm10": data.get("pm10") or data.get("PM10"),
            "no2":  data.get("no2")  or data.get("NO2"),
            "aqi":  data.get("aqi")  or data.get("AQI") or data.get("index"),
            "source": "cityair",
        }
    except Exception as e:
        log.error(f"Ошибка парсинга ответа CityAir для {station['station_id']}: {e}")
        return None


def _fetch_openmeteo_forecast(station: dict) -> list[dict] | None:
    """Запрашивает 7-дневный ежедневный прогноз погоды с Open-Meteo Forecast API.

    Возвращает список из ≤7 записей (один на день) или None при ошибке.
    Поля: forecast_date, temp_max, temp_min, wind_max, precipitation, weathercode.
    """
    data = _get_with_retry(_OPENMETEO_WEATHER_URL, {
        "latitude":        station["latitude"],
        "longitude":       station["longitude"],
        "daily":           "temperature_2m_max,temperature_2m_min,wind_speed_10m_max,"
                           "precipitation_sum,weathercode",
        "wind_speed_unit": "ms",
        "timezone":        "Asia/Novosibirsk",
        "forecast_days":   7,
    })
    if not data or "daily" not in data:
        return None

    daily = data["daily"]
    times    = daily.get("time", [])
    temp_max = daily.get("temperature_2m_max", [])
    temp_min = daily.get("temperature_2m_min", [])
    wind_max = daily.get("wind_speed_10m_max", [])
    precip   = daily.get("precipitation_sum", [])
    wcode    = daily.get("weathercode", [])

    records = []
    for i, day in enumerate(times):
        records.append({
            "forecast_date": str(day),
            "temp_max":      temp_max[i] if i < len(temp_max) else None,
            "temp_min":      temp_min[i] if i < len(temp_min) else None,
            "wind_max":      wind_max[i] if i < len(wind_max) else None,
            "precipitation": precip[i]   if i < len(precip)   else None,
            "weathercode":   int(wcode[i]) if i < len(wcode) and wcode[i] is not None else None,
        })
    return records


def fetch_all_forecast() -> list[dict[str, Any]]:
    """Получает 7-дневный прогноз погоды для всех станций (районов) Новосибирска.

    Возвращает список flat-записей для upsert в eco_forecast.
    """
    from datetime import datetime, timezone
    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []

    for station in NSK_ECOLOGY_STATIONS:
        sid = station["station_id"]
        daily = _fetch_openmeteo_forecast(station)
        if not daily:
            log.warning(f"Прогноз: нет данных для станции {sid}")
            continue
        for day_rec in daily:
            records.append({
                "id":           f"{sid}_{day_rec['forecast_date']}",
                "station_id":   sid,
                "district":     station["district"],
                **day_rec,
                "fetched_at":   fetched_at,
            })

    log.info(f"Forecast ETL: собрано {len(records)} записей ({len(NSK_ECOLOGY_STATIONS)} станций × 7 дней)")
    return records


def fetch_all_ecology() -> list[dict[str, Any]]:
    """Основная ETL-функция: собирает данные со всех источников для всех районов.

    Алгоритм (ТЗ §3.1–3.2):
    1. Для каждой станции запрашивает Open-Meteo (AQ + Weather) параллельно
    2. Если настроен CityAir — обогащает/замещает AQ-данные более точными
    3. Нормализует временные метки → UTC+7 (Новосибирск)
    4. Возвращает плоские dict-записи, готовые для upsert в DuckDB

    Возвращает список dict (fact_measurements + station_id).
    """
    measured_at = datetime.now(timezone.utc).astimezone().isoformat()
    records: list[dict] = []

    for station in NSK_ECOLOGY_STATIONS:
        sid = station["station_id"]

        # Extract
        aq_data      = _fetch_openmeteo_air_quality(station)
        weather_data = _fetch_openmeteo_weather(station)
        cityair_data = _fetch_cityair(station)

        if not aq_data and not weather_data:
            log.warning(f"Нет данных для станции {sid} — пропускаем")
            continue

        # Transform — слияние источников; CityAir имеет приоритет над Open-Meteo AQ
        aq = aq_data or {}
        if cityair_data:
            # Заменяем только непустые поля из CityAir
            for field in ("pm25", "pm10", "no2", "aqi"):
                if cityair_data.get(field) is not None:
                    aq[field] = cityair_data[field]
            aq["source"] = "cityair+open-meteo"
        weather = weather_data or {}

        # Составной ключ (ТЗ §3.3): station_id + временная метка (без дробей секунд)
        ts_key = measured_at[:19].replace(":", "-").replace("T", "_")
        record_id = f"{sid}_{ts_key}"

        records.append({
            "id":               record_id,
            "station_id":       sid,
            "measured_at":      measured_at,
            "pm25":             aq.get("pm25"),
            "pm10":             aq.get("pm10"),
            "no2":              aq.get("no2"),
            "aqi":              aq.get("aqi"),
            "temperature_c":    weather.get("temperature_c"),
            "wind_speed_ms":    weather.get("wind_speed_ms"),
            "wind_direction_deg": weather.get("wind_direction_deg"),
            "humidity_pct":     weather.get("humidity_pct"),
            "pressure_hpa":     weather.get("pressure_hpa"),
            "source":           aq.get("source", "open-meteo"),
        })

    log.info(f"Ecology ETL: собрано {len(records)} записей из {len(NSK_ECOLOGY_STATIONS)} станций")
    return records
