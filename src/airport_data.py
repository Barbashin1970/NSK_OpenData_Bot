"""Статические данные аэропорта Толмачёво (Новосибирск).

IATA: OVB  ICAO: UNNT
Источник: tolmachevo.ru, Новапорт.
Для расписания рейсов — Яндекс.Расписания API (yandex.ru/dev/rasp) — планируется в фазе 2.
"""

AIRPORT_TERMINALS = [
    {
        "id": "A",
        "name": "Терминал A",
        "type": "Внутренние рейсы",
        "airlines": ["S7 Airlines", "Аэрофлот", "Уральские авиалинии", "UTair", "Победа", "Smartavia"],
        "note": "Основной терминал внутренних направлений.",
    },
    {
        "id": "B",
        "name": "Терминал B",
        "type": "Международные рейсы",
        "airlines": ["S7 Airlines", "Turkish Airlines", "Air Arabia", "flydubai", "Казахстан"],
        "note": "Международные направления: ОАЭ, Турция, Таиланд, страны СНГ.",
    },
]

AIRPORT_TRANSPORT = [
    {
        "type": "Экспресс-автобус",
        "routes": ["111э (Аэропорт — пл. Маркса)", "111э через Ленинский р-н"],
        "duration_min": 40,
        "note": "Отправляется от пл. Маркса (Линия 1 метро), ~каждый час.",
    },
    {
        "type": "Обычный автобус",
        "routes": ["342 (Аэропорт — г. Обь — Новосибирск)"],
        "duration_min": 60,
        "note": "Дешевле, останавливается на промежуточных остановках.",
    },
    {
        "type": "Такси",
        "routes": [],
        "duration_min": 30,
        "note": "Яндекс.Такси, Ситимобил. В часы пик — 40–60 мин.",
    },
]

AIRPORT_INFO = {
    "name": "Международный аэропорт Толмачёво",
    "short_name": "Толмачёво",
    "iata": "OVB",
    "icao": "UNNT",
    "city": "Новосибирск",
    # Координаты аэропорта (центр аэродрома)
    "_lon": 82.6508,
    "_lat": 54.9672,
    # Аэропорт расположен в г. Обь, ~17 км к западу от центра Новосибирска по прямой
    "distance_from_center_km": 20,
    "passengers_year_2023": 3_600_000,
    "flights_per_day": 60,
    "terminals": AIRPORT_TERMINALS,
    "transport": AIRPORT_TRANSPORT,
    "operator": "Новапорт",
    "url": "https://tolmachevo.ru",
    "opened_year": 1957,
    "runways": 2,
    "runway_length_m": 3600,  # основная ВПП
}


def get_airport_info() -> dict:
    """Полная информация об аэропорте."""
    return AIRPORT_INFO
