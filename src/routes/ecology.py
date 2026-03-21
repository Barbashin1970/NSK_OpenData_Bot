"""Ecology & weather endpoints."""

import logging
import os
import threading

from fastapi import APIRouter, Query

from ..city_config import get_feature

log = logging.getLogger(__name__)

router = APIRouter()

_ecology_lock = threading.Lock()


def _ecology_auto_update() -> None:
    """Обновляет данные экологии и прогноза если TTL истёк."""
    try:
        from ..ecology_cache import (
            is_ecology_stale, upsert_stations, upsert_measurements,
            is_forecast_stale, upsert_forecast,
        )
        from ..ecology_fetcher import fetch_all_ecology, fetch_all_forecast
        with _ecology_lock:
            if is_ecology_stale():
                upsert_stations()
                upsert_measurements(fetch_all_ecology())
            if is_forecast_stale():
                upsert_forecast(fetch_all_forecast())
    except Exception as e:
        log.error("Ecology auto-update failed: %s", e, exc_info=True)


@router.get(
    "/ecology/status",
    tags=["Экология"],
    summary="Текущее качество воздуха и погода по районам",
    response_description="Массив измерений: PM2.5, PM10, NO2, AQI, температура, ветер, влажность",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "operation": "ECO_STATUS",
                        "district": None,
                        "count": 10,
                        "measured_at": "2026-03-05T10:15:00+07:00",
                        "rows": [
                            {
                                "district": "Советский район",
                                "address": "Академгородок",
                                "pm25": 8.2,
                                "pm10": 15.1,
                                "no2": 12.0,
                                "aqi": 32,
                                "temperature_c": -5.0,
                                "wind_speed_ms": 2.1,
                                "humidity_pct": 78,
                                "source": "open-meteo-aq",
                                "measured_at": "2026-03-05T10:15:00+07:00",
                            }
                        ],
                        "columns": ["district", "address", "pm25", "pm10", "no2", "aqi",
                                    "temperature_c", "wind_speed_ms", "humidity_pct", "source", "measured_at"],
                        "ecology_meta": {
                            "last_updated": "2026-03-05T10:15:00+07:00",
                            "total_records": 80,
                            "districts_covered": 10,
                        },
                    }
                }
            }
        }
    },
)
def get_ecology_status(
    district: str | None = Query(
        None,
        description=(
            "Фильтр по району. Примеры: `Советский район`, `Центральный район`.\n\n"
            "Без параметра — возвращаются данные по всем 10 районам Новосибирска."
        ),
        examples={"default": {"summary": "Академгородок", "value": "Советский район"}},
    ),
) -> dict:
    """
    Возвращает текущий снимок качества воздуха и погоды по всем районам (или одному).

    Данные обновляются автоматически если TTL (15 мин) истёк.

    ### Поля каждой записи

    | Поле | Тип | Описание |
    |---|---|---|
    | `district` | string | Административный район |
    | `address` | string | Описание точки мониторинга |
    | `pm25` | float | PM2.5, мкг/м³ |
    | `pm10` | float | PM10, мкг/м³ |
    | `no2` | float | NO2 (диоксид азота), мкг/м³ |
    | `aqi` | int | Европейский индекс качества воздуха (0–500) |
    | `temperature_c` | float | Температура воздуха, °C |
    | `wind_speed_ms` | float | Скорость ветра, м/с |
    | `humidity_pct` | float | Относительная влажность, % |
    | `source` | string | Источник: `open-meteo-aq`, `cityair`, `cityair+open-meteo` |
    | `measured_at` | string | Время измерения (ISO 8601) |

    ### Интерпретация AQI (European AQI)

    | AQI | Категория |
    |---|---|
    | 0–20 | Отличный |
    | 20–40 | Хороший |
    | 40–60 | Умеренный |
    | 60–80 | Плохой |
    | 80–100 | Очень плохой |
    | >100 | Экстремальный |

    Эквивалентно запросу: `GET /ask?q=качество+воздуха+сейчас`
    """
    _ecology_auto_update()
    from ..ecology_cache import query_current, get_ecology_meta
    rows = query_current(district_filter=district)
    meta = get_ecology_meta()
    cols = ["district", "address", "pm25", "pm10", "no2", "aqi",
            "temperature_c", "wind_speed_ms", "humidity_pct", "source", "measured_at"]
    return {
        "operation": "ECO_STATUS",
        "district": district,
        "count": len(rows),
        "measured_at": meta.get("last_updated", ""),
        "rows": [{k: r.get(k) for k in cols} for r in rows],
        "columns": cols,
        "ecology_meta": meta,
    }


@router.get(
    "/ecology/pdk",
    tags=["Экология"],
    summary="Превышения ПДК PM2.5 (порог ВОЗ: 35 мкг/м³)",
    response_description="Районы с PM2.5 > 35 мкг/м³ за текущие сутки",
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "no_exceedance": {
                            "summary": "Нет превышений",
                            "value": {
                                "operation": "ECO_PDK",
                                "threshold_pm25": 35.0,
                                "standard": "WHO Air Quality Guidelines 2021",
                                "count": 0,
                                "rows": [],
                                "columns": ["district", "pm25_max", "pm25_avg", "измерений", "последнее"],
                            },
                        },
                        "has_exceedance": {
                            "summary": "Есть превышение",
                            "value": {
                                "operation": "ECO_PDK",
                                "threshold_pm25": 35.0,
                                "count": 1,
                                "rows": [
                                    {
                                        "district": "Ленинский район",
                                        "pm25_max": 52.3,
                                        "pm25_avg": 41.7,
                                        "измерений": 8,
                                        "последнее": "2026-03-05T09:00:00+07:00",
                                    }
                                ],
                            },
                        },
                    }
                }
            }
        }
    },
)
def get_ecology_pdk(
    district: str | None = Query(
        None,
        description="Фильтр по конкретному району. Без параметра — все районы.",
    ),
) -> dict:
    """
    Возвращает районы, где PM2.5 превысил порог 35 мкг/м³ за текущие сутки.

    Порог 35 мкг/м³ — среднесуточный стандарт ВОЗ (WHO Air Quality Guidelines, 2021).
    Российский ПДК среднесуточный — 25 мкг/м³.

    ### Поля ответа

    | Поле | Тип | Описание |
    |---|---|---|
    | `district` | string | Район с превышением |
    | `pm25_max` | float | Максимальное зафиксированное значение PM2.5 за сутки |
    | `pm25_avg` | float | Среднее значение PM2.5 за сутки |
    | `измерений` | int | Количество снимков за сутки |
    | `последнее` | string | Время последнего измерения |

    Эквивалентно запросу: `GET /ask?q=превышение+ПДК+PM2.5`
    """
    _ecology_auto_update()
    from ..ecology_cache import query_pdk_exceedances
    rows = query_pdk_exceedances(district_filter=district)
    cols = ["district", "pm25_max", "pm25_avg", "измерений", "последнее"]
    return {
        "operation": "ECO_PDK",
        "threshold_pm25": 35.0,
        "standard": "WHO Air Quality Guidelines 2021",
        "district": district,
        "count": len(rows),
        "rows": [{k: r.get(k) for k in cols} for r in rows],
        "columns": cols,
        "note": "PM2.5 > 35 мкг/м³ — суточный порог ВОЗ",
    }


@router.get(
    "/ecology/history",
    tags=["Экология"],
    summary="История качества воздуха по дням",
    response_description="Агрегированные показатели PM2.5, AQI, погоды по дням и районам",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "operation": "ECO_HISTORY",
                        "days": 7,
                        "district": "Советский район",
                        "count": 7,
                        "rows": [
                            {
                                "день": "2026-03-05",
                                "район": "Советский район",
                                "pm25_ср": 9.1,
                                "pm25_макс": 14.2,
                                "aqi_ср": 28,
                                "темп_ср": -4.5,
                                "ветер_ср": 2.3,
                                "снимков": 24,
                            }
                        ],
                        "columns": ["день", "район", "pm25_ср", "pm25_макс", "aqi_ср",
                                    "темп_ср", "ветер_ср", "снимков"],
                    }
                }
            }
        }
    },
)
def get_ecology_history(
    days: int = Query(
        7,
        ge=1,
        le=7,
        description="Глубина истории в днях (1–7). История хранится 7 дней.",
    ),
    district: str | None = Query(
        None,
        description="Фильтр по району. Без параметра — все районы.",
        examples={"default": {"summary": "Советский район", "value": "Советский район"}},
    ),
) -> dict:
    """
    Возвращает агрегированные данные о качестве воздуха и погоде по дням.

    Полезно для анализа динамики: «Какой был PM2.5 в Советском районе за неделю?»
    Поле `ветер_ср` позволяет коррелировать скорость ветра с уровнем PM2.5
    (слабый ветер → накопление смога).

    ### Поля каждой записи

    | Поле | Тип | Описание |
    |---|---|---|
    | `день` | string | Дата (YYYY-MM-DD) |
    | `район` | string | Административный район |
    | `pm25_ср` | float | Среднее PM2.5 за день, мкг/м³ |
    | `pm25_макс` | float | Максимальное PM2.5 за день, мкг/м³ |
    | `aqi_ср` | float | Средний AQI за день |
    | `темп_ср` | float | Средняя температура, °C |
    | `ветер_ср` | float | Средняя скорость ветра, м/с |
    | `снимков` | int | Количество измерений в день |

    Эквивалентно запросу: `GET /ask?q=динамика+PM2.5+за+неделю`
    """
    from ..ecology_cache import query_history
    rows = query_history(district_filter=district, days=days)
    cols = ["день", "район", "pm25_ср", "pm25_макс", "aqi_ср", "темп_ср", "ветер_ср", "снимков"]
    return {
        "operation": "ECO_HISTORY",
        "days": days,
        "district": district,
        "count": len(rows),
        "rows": [{k: r.get(k) for k in cols} for r in rows],
        "columns": cols,
    }


@router.post(
    "/ecology/update",
    tags=["Экология"],
    summary="Обновить данные о качестве воздуха и погоде",
    response_description="Статус обновления: количество загруженных измерений",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "records_loaded": 10,
                        "districts_covered": 10,
                        "last_updated": "2026-03-05T10:15:00+07:00",
                        "source": "open-meteo",
                    }
                }
            }
        }
    },
)
def post_ecology_update() -> dict:
    """
    Принудительно загружает актуальные данные с Open-Meteo (и CityAir если настроен ключ).

    В штатном режиме обновление происходит автоматически при каждом запросе к `/ecology/*`
    и `/ask` (тема `ecology`) если TTL (15 мин) истёк.

    ### Что происходит

    1. Запрос к [Open-Meteo Air Quality API](https://air-quality-api.open-meteo.com) — PM2.5, PM10, NO2, AQI
    2. Запрос к [Open-Meteo Forecast API](https://api.open-meteo.com) — температура, ветер, давление
    3. Опционально: запрос к CityAir API (если задан `CITYAIR_API_KEY` в `.env`)
    4. Upsert в DuckDB таблицы `fact_measurements` по всем 10 районам

    > CityAir обогащает данные Open-Meteo если API-ключ задан в переменной `CITYAIR_API_KEY`.
    """
    from ..ecology_fetcher import fetch_all_ecology
    from ..ecology_cache import upsert_stations, upsert_measurements, get_ecology_meta

    upsert_stations()
    records = fetch_all_ecology()
    count = upsert_measurements(records)
    meta = get_ecology_meta()
    has_cityair = bool(os.environ.get("CITYAIR_API_KEY", "").strip())
    return {
        "success": count > 0,
        "records_loaded": count,
        "districts_covered": meta.get("districts_covered", 0),
        "last_updated": meta.get("last_updated", ""),
        "source": "open-meteo+cityair" if has_cityair else "open-meteo",
    }


@router.get(
    "/ecology/risks",
    tags=["Экология"],
    summary="Прескриптивная аналитика: карточки рисков + рекомендации",
    response_description="Список активных рисков с рекомендациями для горожан и диспетчеров",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "operation": "ECO_RISKS",
                        "district": None,
                        "count": 2,
                        "risks": [
                            {
                                "id": "smog_trap",
                                "scenario": "Экологическая ловушка",
                                "severity": "warning",
                                "icon": "🌫️",
                                "title": "Безветрие блокирует рассеивание выбросов",
                                "metrics": "Ветер 0.8 м/с · PM2.5 28.3 мкг/м³",
                                "citizen": "Не открывайте окна ночью...",
                                "official": "Рассмотреть объявление режима НМУ...",
                            }
                        ],
                        "ecology_meta": {"last_updated": "2026-03-05T10:15:00+07:00"},
                    }
                }
            }
        }
    },
)
def get_ecology_risks(
    district: str | None = Query(
        None,
        description="Фильтр по району. Без параметра — анализ по всем районам.",
    ),
) -> dict:
    """
    Прескриптивная аналитика: вычисляет активные риски на основе текущих данных.

    ### Обнаруживаемые сценарии

    | ID | Сценарий | Триггер |
    |---|---|---|
    | `smog_trap` | Экологическая ловушка | Ветер < 1.5 м/с + PM2.5 > 20 мкг/м³ |
    | `pdk` | Превышение нормы ВОЗ | PM2.5 > 35 мкг/м³ |
    | `ice` | Риск гололёда | Температура от −3°C до +2°C |
    | `temp_shock` | Температурный шок | Суточная дельта ≤ −15°C |
    | `severe_cold` | Экстремальный холод | Температура < −20°C |

    ### Поля каждого риска

    | Поле | Описание |
    |---|---|
    | `id` | Идентификатор сценария |
    | `severity` | `warning` или `critical` |
    | `icon` | Эмодзи-иконка |
    | `title` | Краткое описание |
    | `metrics` | Значения, вызвавшие триггер |
    | `citizen` | Рекомендация для горожанина |
    | `official` | Рекомендация для диспетчера мэрии |

    Эквивалентно запросу: `GET /ask?q=риски+для+жизни+в+городе`
    """
    _ecology_auto_update()
    from ..ecology_cache import query_risks, get_ecology_meta
    risks = query_risks(district_filter=district)
    meta = get_ecology_meta()
    return {
        "operation": "ECO_RISKS",
        "district": district,
        "count": len(risks),
        "risks": risks,
        "ecology_meta": meta,
    }


# ── Life indices (driven by ecology data) ────────────────────────────────────

def _compute_life_indices(rows: list[dict]) -> dict | None:
    """Вычисляет индексы водителя, прогулки и коммунального напряжения."""
    if not rows:
        return None

    from ..rule_engine import rules

    def _avg(lst):
        return round(sum(lst) / len(lst), 2) if lst else None

    def _min(lst):
        return round(min(lst), 2) if lst else None

    def _vals(key):
        return [float(r[key]) for r in rows if r.get(key) is not None]

    avg_t = _avg(_vals("temperature_c"))
    avg_w = _avg(_vals("wind_speed_ms"))
    avg_p = _avg(_vals("pm25"))
    avg_a = _avg(_vals("aqi"))
    min_t = _min(_vals("temperature_c"))

    cfg = rules.get("life_indices_rules")
    dr  = cfg.get("driver",  {})
    wk  = cfg.get("walk",    {})
    ut  = cfg.get("utility", {})

    # ── Индекс водителя ───────────────────────────────────────────────────────
    driver = 0
    if avg_t is not None:
        bi_min = float(dr.get("temp_black_ice_min", -3.0))
        bi_max = float(dr.get("temp_black_ice_max",  2.0))
        if bi_min <= avg_t <= bi_max:
            driver += int(dr.get("temp_black_ice_score", 5))
        elif float(dr.get("temp_regular_ice_min", -10.0)) <= avg_t < bi_min:
            driver += int(dr.get("temp_regular_ice_score", 2))
        if avg_t < float(dr.get("temp_extreme_cold", -25.0)):
            driver += int(dr.get("temp_extreme_cold_score", 3))
        elif avg_t < float(dr.get("temp_severe_cold", -20.0)):
            driver += int(dr.get("temp_severe_cold_score", 2))
        elif avg_t < float(dr.get("temp_cold", -10.0)):
            driver += int(dr.get("temp_cold_score", 1))
    if avg_w is not None:
        if avg_w > float(dr.get("wind_strong_ms", 12.0)):
            driver += int(dr.get("wind_strong_score", 2))
        elif avg_w > float(dr.get("wind_moderate_ms", 8.0)):
            driver += int(dr.get("wind_moderate_score", 1))
    driver = min(10, max(0, round(driver)))

    driver_levels = dr.get("levels", [])
    driver_label = next((l["label"] for l in driver_levels if driver <= l["max"]), "Крайне опасно")

    # ── Индекс прогулки ───────────────────────────────────────────────────────
    walk = int(wk.get("start_score", 10))
    if avg_p is not None:
        if avg_p > float(wk.get("pm25_high", 35.0)):    walk -= int(wk.get("pm25_high_penalty",   4))
        elif avg_p > float(wk.get("pm25_medium", 25.0)): walk -= int(wk.get("pm25_medium_penalty", 2))
        elif avg_p > float(wk.get("pm25_low", 15.0)):    walk -= int(wk.get("pm25_low_penalty",    1))
    if avg_a is not None:
        if avg_a > float(wk.get("aqi_very_high", 80)):   walk -= int(wk.get("aqi_very_high_penalty", 3))
        elif avg_a > float(wk.get("aqi_high", 60)):      walk -= int(wk.get("aqi_high_penalty",      2))
        elif avg_a > float(wk.get("aqi_medium", 40)):    walk -= int(wk.get("aqi_medium_penalty",    1))
    if avg_t is not None:
        if avg_t < float(wk.get("temp_extreme", -25.0)):  walk -= int(wk.get("temp_extreme_penalty", 3))
        elif avg_t < float(wk.get("temp_cold", -15.0)):   walk -= int(wk.get("temp_cold_penalty",    2))
        elif avg_t < float(wk.get("temp_chilly", -5.0)):  walk -= int(wk.get("temp_chilly_penalty",  1))
    if avg_w is not None and avg_w > float(wk.get("wind_threshold_ms", 10.0)):
        walk -= int(wk.get("wind_penalty", 1))
    walk = min(10, max(0, round(walk)))

    walk_levels = wk.get("levels", [])
    walk_label  = next((l["label"] for l in walk_levels if walk >= l["min"]), "Рекомендуется остаться дома")

    # ── Индекс коммунального напряжения ───────────────────────────────────────
    utility = 0
    if min_t is not None:
        if min_t < float(ut.get("temp_critical", -30.0)):  utility += int(ut.get("temp_critical_score", 4))
        elif min_t < float(ut.get("temp_severe", -20.0)):  utility += int(ut.get("temp_severe_score",   3))
        elif min_t < float(ut.get("temp_cold", -10.0)):    utility += int(ut.get("temp_cold_score",     2))
        elif min_t < float(ut.get("temp_cool", -5.0)):     utility += int(ut.get("temp_cool_score",     1))
    if avg_w is not None and avg_p is not None:
        if avg_w < float(ut.get("smog_wind_threshold_ms", 1.5)) and avg_p > float(ut.get("smog_pm25_threshold", 20.0)):
            utility += int(ut.get("smog_score", 2))
    utility = min(10, max(0, round(utility)))

    util_levels = ut.get("levels", [])
    util_label  = next((l["label"] for l in util_levels if utility <= l["max"]), "Критично")

    return {
        "driver":  {"score": driver,  "label": driver_label,  "icon": "🚗", "name": "Индекс водителя"},
        "walk":    {"score": walk,    "label": walk_label,    "icon": "🚶", "name": "Индекс прогулки"},
        "utility": {"score": utility, "label": util_label,    "icon": "🏗️",  "name": "Коммунальное напряжение"},
        "inputs":  {"avg_t": avg_t, "avg_w": avg_w, "avg_p": avg_p, "avg_a": avg_a, "min_t": min_t},
    }


@router.get(
    "/life-indices",
    tags=["Экология и погода"],
    summary="Индексы жизни города (водитель / прогулка / коммуналка)",
)
def get_life_indices() -> dict:
    """Три синтетических индекса на основе текущих экологических данных.

    Параметры берутся из **config/rules/life_indices_rules.yaml** —
    можно изменить пороги и применить через `POST /admin/reload-rules`.

    | Индекс | Шкала | Смысл |
    |---|---|---|
    | Водитель | 0–10 (выше = опаснее) | Риск для автомобилиста |
    | Прогулка | 0–10 (выше = лучше) | Комфорт прогулки на улице |
    | Коммунальное напряжение | 0–10 (выше = хуже) | Нагрузка на ЖКХ и теплосети |
    """
    from ..ecology_cache import query_current
    rows = query_current()
    result = _compute_life_indices(rows)
    if result is None:
        return {"error": "Нет данных экологии", "hint": "POST /update — обновить данные"}
    return result
