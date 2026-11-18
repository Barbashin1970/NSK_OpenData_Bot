"""Ecology & weather endpoints."""

import logging
import os
import threading

from fastapi import APIRouter, HTTPException, Query

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

    # Входы индексов жизни на табло: гололёд по влаге, ночной минимум, снегопад на 3 суток.
    # Нет прогноза — null, а не 0 (сверка с РАГРАФ 18.11): табло пишет «неполные данные».
    from ..ecology_cache import life_index_inputs
    inputs = life_index_inputs(district)

    return {
        "operation": "ECO_STATUS",
        "district": district,
        "count": len(rows),
        "measured_at": meta.get("last_updated", ""),
        "rows": [{k: r.get(k) for k in cols} for r in rows],
        "columns": cols,
        "ecology_meta": meta,
        "snow_impact": inputs["snow_impact"],
        "ice_level": inputs["ice_level"],
        "night_min": inputs["night_min"],
    }


@router.get(
    "/ecology/pdk",
    tags=["Экология"],
    summary="Превышения ПДК PM2.5 (среднесуточная ПДК: 35 мкг/м³)",
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

    Порог 35 мкг/м³ — среднесуточная ПДК PM2.5 по СанПиН 1.2.3685-21 (0,035 мг/м³). Ориентир ВОЗ 2021 строже — 15 мкг/м³.
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
        "note": "PM2.5 > 35 мкг/м³ — среднесуточная ПДК",
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
        le=365,
        description=(
            "Глубина истории в днях (1–365). Хранение: 30 дней почасовых снимков "
            "+ 365 дней суточного архива. Реально доступная глубина ограничена "
            "тем, сколько накоплено — см. поле `days_observed` в ответе."
        ),
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
    observed = sorted({r["день"] for r in rows if r.get("день")})
    return {
        "operation": "ECO_HISTORY",
        "days": days,
        "district": district,
        # Фактическая глубина: сбор прерывался, календарный период больше
        "days_observed": len(observed),
        "period_from": observed[0] if observed else None,
        "period_to": observed[-1] if observed else None,
        "count": len(rows),
        "rows": [{k: r.get(k) for k in cols} for r in rows],
        "columns": cols,
    }


@router.get(
    "/ecology/aqi-exceedances",
    tags=["Экология"],
    summary="История превышений AQI по часам и дням",
)
def get_aqi_exceedance_history(
    aqi_threshold: int = Query(
        40,
        ge=20,
        le=200,
        description="Порог AQI (по умолчанию 40 — начало категории «Умеренный»)",
    ),
    days: int = Query(
        30,
        ge=1,
        le=365,
        description=(
            "Глубина истории в днях (1–365). ВНИМАНИЕ: почасовая детализация "
            "живёт только в fact_measurements со сроком хранения 30 дней, "
            "поэтому фактическое окно — не больше 30 дней независимо от запроса "
            "(см. `hourly_window_days` в ответе)."
        ),
    ),
    district: str | None = Query(
        None,
        description="Фильтр по району",
    ),
) -> dict:
    """
    Почасовая история превышений AQI за N дней.

    Используется для:
    - построения графиков загрязнения по часам суток (паттерны утро/вечер)
    - анализа дней с плохим воздухом
    - прогнозной аналитики по паттернам

    Суточный архив хранится 365 дней, но почасовая разбивка — только 30:
    за пределами этого окна остаются лишь дневные агрегаты (`/ecology/history`).
    """
    from ..constants import ECOLOGY_HISTORY_DAYS
    from ..ecology_cache import query_aqi_exceedance_history
    rows = query_aqi_exceedance_history(
        aqi_threshold=aqi_threshold, days=days, district_filter=district,
    )

    # Сводка по дням
    day_summary: dict = {}
    for r in rows:
        d = r["day"]
        if d not in day_summary:
            day_summary[d] = {"day": d, "hours_exceeded": 0, "aqi_max": 0, "districts": set()}
        day_summary[d]["hours_exceeded"] += 1
        day_summary[d]["aqi_max"] = max(day_summary[d]["aqi_max"], r["aqi_max"])
        day_summary[d]["districts"].add(r["district"])
    summary = sorted(day_summary.values(), key=lambda x: x["day"], reverse=True)
    for s in summary:
        s["districts"] = sorted(s["districts"])

    return {
        "operation": "AQI_EXCEEDANCE_HISTORY",
        "aqi_threshold": aqi_threshold,
        "days": days,
        # Реальное окно почасовых данных — retention fact_measurements
        "hourly_window_days": min(days, ECOLOGY_HISTORY_DAYS),
        "district": district,
        "total_records": len(rows),
        "days_exceeded": len(summary),
        "hourly": rows,
        "daily_summary": summary,
    }


@router.get(
    "/ecology/rating-districts",
    tags=["Экология"],
    summary="Рейтинг районов по экологии за период (A-F)",
    response_description="Сравнительная таблица всех районов: AQI, PM2.5, грейд A-F",
)
def get_ecology_rating_districts(
    days: int = Query(
        30, ge=7, le=365,
        description=(
            "Глубина анализа в днях (7–365). Считается по суточному архиву, "
            "поэтому доступна вся накопленная глубина; фактическое число дней "
            "с данными по каждому району — в поле `days_total` строки."
        ),
    ),
) -> dict:
    """Рейтинг районов по качеству воздуха за период.

    Для каждого района:
      - score 0-10 + грейд A-F
      - AQI: среднее, max, min
      - PM2.5: среднее, max
      - Дней: всего, чистых (AQI ≤ 35), с превышениями (AQI > 50)
      - Температура и ветер за период (для контекста)
    """
    from ..ecology_cache import query_district_ecology_rating
    rows = query_district_ecology_rating(days=days)
    return {
        "operation": "ECO_RATING_DISTRICTS",
        "days": days,
        # Максимум наблюдённых дней среди районов — честный знаменатель для UI
        "days_observed": max((r.get("days_total") or 0 for r in rows), default=0),
        "count": len(rows),
        "rows": rows,
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

    try:
        with _ecology_lock:
            upsert_stations()
            records = fetch_all_ecology()
            count = upsert_measurements(records)
    except Exception as exc:
        log.error("ecology/update failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка обновления экологии: {exc}")

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
    "/ecology/snow-outlook",
    tags=["Экология"],
    summary="Снегопад по районам на 1–3 суток: уровни, действия ДЭУ, тексты УК и жителям (пилот)",
    response_description=(
        "Районы × сутки: снег по прогнозу, уровень (2 / 5 / 15 см), действия с нормами и сроками, "
        "итог ретропроверки прогноза снега (docs/SNEGOPAD-RAZBOR.md)"
    ),
)
def get_snow_outlook(
    district: str | None = Query(
        None, description="Район (по первому слову, Академгородок — Советский); пусто — весь город",
    ),
) -> dict:
    """Прогноз снегопада по районам с завтрашних суток (регламент ecology_rules, snow_outlook).

    Прогноз — модель Open-Meteo, а не прогноз УГМС: повод готовиться, а не основание для сроков
    по норме. Поле `retro` — насколько прогнозу снега верить по прошлым зимам (против ERA5).
    """
    from ..ecology_cache import query_snow_outlook
    try:
        return query_snow_outlook(district or None)
    except Exception as e:
        # сбой расчёта и «снега нет» — разные вещи: отдаём ошибку явно
        log.error("snow outlook error: %s", e)
        return {"operation": "SNOW_OUTLOOK", "error": str(e), "district": district,
                "days": [], "districts": [], "city": [], "actions": [], "max_level": 0, "worst": None}


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
    | `pdk` | Превышение ПДК PM2.5 | PM2.5 > 35 мкг/м³ (среднесуточная ПДК) |
    | `ice` | Гололёд / возможна гололедица | ледяной дождь или морось в прогнозе на сегодня; либо днём дождь от 1 мм, а ночью мороз до −1 °C |
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

def _compute_life_indices(rows: list[dict], ice_level: str | None = None,
                          snow_impact: float | None = None,
                          night_min: float | None = None) -> dict | None:
    """Вычисляет индексы водителя, прогулки и погодной нагрузки на ЖКХ.

    ice_level — гололёд на сегодня по влаге (ecology_cache.ice_level: «glaze» | «refreeze» | None);
    snow_impact — индекс снегопада 0–10 из прогноза на 3 суток (как на табло);
    night_min — ночной минимум, °C: минимум следующих суток прогноза.
    Входы прогноза — ecology_cache.life_index_inputs. None — прогноза нет (⊥), а не 0: такой
    вход в балл не идёт, индекс помечен partial и называет, чего не хватает (missing).
    """
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
    # Мороз и ветер; гололёд — отдельной ветвью по влаге, как в РАГРАФ (18.11). Прежние «зоны
    # льда» по одной температуре (−3…+2 °C: +5, −10…−3 °C: +2) убраны: в сухую погоду около нуля
    # льда нет, и правило не прошло проверку на ДТП (РАГРАФ-ПРОГНОЗ.НИР.001-2026, п. 9.2).
    driver = 0
    if ice_level == "glaze":
        driver += int(dr.get("ice_glaze_score", 5))
    elif ice_level == "refreeze":
        driver += int(dr.get("ice_refreeze_score", 3))
    if avg_t is not None:
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
    # снегопад: видимость и сцепление — как на табло (computeLifeIndices). Нет прогноза — ни
    # снегопад, ни гололёд не оценены: балл по морозу и ветру с пометкой «неполные данные»
    if snow_impact is not None:
        if snow_impact >= float(dr.get("snow_heavy_threshold", 8)):
            driver += int(dr.get("snow_heavy_score", 2))
        elif snow_impact >= float(dr.get("snow_moderate_threshold", 5)):
            driver += int(dr.get("snow_moderate_score", 1))
    driver = min(10, max(0, round(driver)))
    driver_missing = "" if snow_impact is not None else "нет прогноза: гололёд и снегопад не оценены"

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

    # ── Погодная нагрузка на ЖКХ (было «Коммунальное напряжение», 18.11) ────────
    # Мороз по ночному минимуму — теплосети; снегопад по прогнозу — уборка и кровли. «Штиль +
    # загрязнение» убран: смог — это экология (учтён в «Прогулке»), а воздух — модель CAMS.
    # Регламент 1.3 (сверка с РАГРАФ): мороз — по night_min из прогноза, а не по текущей
    # температуре; обеих величин нет — балла нет («нет прогноза»), одной — «неполные данные».
    utility = None if night_min is None and snow_impact is None else 0
    if night_min is not None:
        nm = float(night_min)
        if nm < float(ut.get("temp_critical", -30.0)):  utility += int(ut.get("temp_critical_score", 4))
        elif nm < float(ut.get("temp_severe", -20.0)):  utility += int(ut.get("temp_severe_score",   3))
        elif nm < float(ut.get("temp_cold", -10.0)):    utility += int(ut.get("temp_cold_score",     2))
        elif nm < float(ut.get("temp_cool", -5.0)):     utility += int(ut.get("temp_cool_score",     1))
    if snow_impact is not None:
        if snow_impact >= float(ut.get("snow_heavy_threshold", 8)):
            utility += int(ut.get("snow_heavy_score", 3))
        elif snow_impact >= float(ut.get("snow_moderate_threshold", 5)):
            utility += int(ut.get("snow_moderate_score", 2))
        elif snow_impact >= float(ut.get("snow_light_threshold", 2)):
            utility += int(ut.get("snow_light_score", 1))
    if utility is not None:
        utility = min(10, max(0, round(utility)))
    nehvataet = [n for n, v in (("ночной минимум", night_min), ("снегопад", snow_impact)) if v is None]
    util_missing = ("нет прогноза: " + ", ".join(nehvataet)) if nehvataet else ""

    util_levels = ut.get("levels", [])
    if utility is None:
        util_label = "Нет прогноза"
    else:
        util_label = next((l["label"] for l in util_levels if utility <= l["max"]),
                          util_levels[-1]["label"] if util_levels else "Высокая")

    return {
        "driver":  {"score": driver,  "label": driver_label,  "icon": "🚗", "name": "Индекс водителя", "short": "водителя",
                    "partial": bool(driver_missing), "missing": driver_missing},
        "walk":    {"score": walk,    "label": walk_label,    "icon": "🚶", "name": "Индекс прогулки", "short": "прогулки",
                    "partial": False, "missing": ""},
        "utility": {"score": utility, "label": util_label,    "icon": "🏗️",  "name": "Погодная нагрузка на ЖКХ",
                    "short": "ЖКХ: погода", "partial": bool(util_missing), "missing": util_missing,
                    "hint": "Сколько погода нагружает ЖКХ: мороз по ночному минимуму из прогноза — теплосети, снегопад по прогнозу — уборка и кровли. Расчётная температура отопления Новосибирска −37 °C (СП 131.13330.2018). Это не загрузка бригад — её показывает прогноз ЖКХ на 2 недели."},
        "inputs":  {"avg_t": avg_t, "avg_w": avg_w, "avg_p": avg_p, "avg_a": avg_a, "min_t": min_t,
                    "night_min": night_min, "snow_impact": snow_impact, "ice_level": ice_level},
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
    | Погодная нагрузка на ЖКХ | 0–10 (выше = хуже) | Мороз (теплосети) и снегопад (уборка, кровли) — только погода, не загрузка бригад |
    """
    from ..ecology_cache import life_index_inputs, query_current
    rows = query_current()
    # входы прогноза — те же, что у /ecology/status для табло; нет прогноза — None, а не 0
    result = _compute_life_indices(rows, **life_index_inputs(None))
    if result is None:
        return {"error": "Нет данных экологии", "hint": "POST /update — обновить данные"}
    return result
