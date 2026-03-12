"""Статические данные Новосибирского метрополитена.

2 линии, 13 станций. Открыт в 1985 году.
Координаты приблизительные (WGS-84), пригодны для отображения на карте.
Источник: nsk-metro.ru, Wikipedia.
"""

METRO_INFO = {
    "name": "Новосибирский метрополитен",
    "city": "Новосибирск",
    "lines_count": 2,
    "stations_count": 13,
    "daily_passengers": 243_000,
    "opened_year": 1985,
    "fare_rub": 30,
    "url": "https://www.nsk-metro.ru",
    # Метромост: открытая эстакада над рекой Обь (822 м) — входит в маршрут Линии 1
    "bridge_length_m": 822,
}

METRO_LINES: dict = {
    "1": {
        "name": "Дзержинская",
        "color": "#2563eb",       # синяя
        "color_name": "синяя",
        "stations": 8,
        "termini": ["Заельцовская", "Площадь Маркса"],
    },
    "2": {
        "name": "Ленинская",
        "color": "#dc2626",       # красная
        "color_name": "красная",
        "stations": 5,
        "termini": ["Берёзовая роща", "Площадь Гарина-Михайловского"],
    },
}

# Станции в порядке следования (от северного конца к южному / от восточного к западному)
# interchange_with: список пар [номер_линии, название_станции] — пересадки
METRO_STATIONS: list[dict] = [
    # ── Линия 1 — Дзержинская (синяя) ──────────────────────────────────────────
    {
        "name": "Заельцовская",
        "line": "1",
        "_lon": 82.934, "_lat": 54.996,
        "address": "ул. Дуси Ковальчук, 179",
        "district": "Заельцовский район",
        "interchange_with": [],
        "note": "Конечная. Рядом — Заельцовский парк и зоопарк.",
        "passengers_day": 29_000,
    },
    {
        "name": "Гагаринская",
        "line": "1",
        "_lon": 82.931, "_lat": 54.987,
        "address": "ул. Гагарина, 91",
        "district": "Калининский район",
        "interchange_with": [],
        "note": None,
        "passengers_day": None,
    },
    {
        "name": "Красный проспект",
        "line": "1",
        "_lon": 82.925, "_lat": 54.979,
        "address": "Красный просп., 13",
        "district": "Центральный район",
        "interchange_with": [],
        "note": "Центральная деловая улица города.",
        "passengers_day": None,
    },
    {
        "name": "Площадь Ленина",
        "line": "1",
        "_lon": 82.919, "_lat": 54.971,
        "address": "пл. Ленина, 1",
        "district": "Центральный район",
        "interchange_with": [["2", "Сибирская"]],
        "note": "Пересадка на Линию 2 (Сибирская). Главная площадь города.",
        "passengers_day": None,
    },
    {
        "name": "Октябрьская",
        "line": "1",
        "_lon": 82.904, "_lat": 54.961,
        "address": "ул. Октябрьская, 24",
        "district": "Центральный район",
        "interchange_with": [],
        "note": None,
        "passengers_day": None,
    },
    {
        "name": "Речной вокзал",
        "line": "1",
        "_lon": 82.896, "_lat": 54.952,
        "address": "Красный просп., 1/1",
        "district": "Центральный район",
        "interchange_with": [],
        "note": "Рядом — набережная реки Обь и речной вокзал.",
        "passengers_day": None,
    },
    {
        "name": "Студенческая",
        "line": "1",
        "_lon": 82.871, "_lat": 54.929,
        "address": "пр. Карла Маркса, 7",
        "district": "Октябрьский район",
        "interchange_with": [],
        "note": "После Речного вокзала линия пересекает Обь по Метромосту (822 м).",
        "passengers_day": None,
    },
    {
        "name": "Площадь Маркса",
        "line": "1",
        "_lon": 82.894, "_lat": 54.861,
        "address": "пл. Карла Маркса, 1",
        "district": "Октябрьский район",
        "interchange_with": [],
        "note": "Конечная. Крупнейший торговый узел левого берега.",
        "passengers_day": 43_000,
    },

    # ── Линия 2 — Ленинская (красная) ───────────────────────────────────────────
    {
        "name": "Берёзовая роща",
        "line": "2",
        "_lon": 82.975, "_lat": 54.989,
        "address": "ул. Кошурникова, 1",
        "district": "Дзержинский район",
        "interchange_with": [],
        "note": "Конечная восточного направления.",
        "passengers_day": None,
    },
    {
        "name": "Маршала Покрышкина",
        "line": "2",
        "_lon": 82.963, "_lat": 54.984,
        "address": "ул. Кошурникова, 23",
        "district": "Дзержинский район",
        "interchange_with": [],
        "note": None,
        "passengers_day": None,
    },
    {
        "name": "Золотая нива",
        "line": "2",
        "_lon": 82.948, "_lat": 54.981,
        "address": "ул. Кошурникова, 50",
        "district": "Дзержинский район",
        "interchange_with": [],
        "note": None,
        "passengers_day": None,
    },
    {
        "name": "Площадь Гарина-Михайловского",
        "line": "2",
        "_lon": 82.929, "_lat": 54.968,
        "address": "пл. Гарина-Михайловского, 1",
        "district": "Центральный район",
        "interchange_with": [],
        "note": "Конечная. Рядом — главный железнодорожный вокзал «Новосибирск-Главный».",
        "passengers_day": None,
    },
    {
        "name": "Сибирская",
        "line": "2",
        "_lon": 82.921, "_lat": 54.971,
        "address": "ул. 1905 года, 1",
        "district": "Центральный район",
        "interchange_with": [["1", "Площадь Ленина"]],
        "note": "Пересадка на Линию 1 (Площадь Ленина). Подземный переход.",
        "passengers_day": None,
    },
]


def get_metro_info() -> dict:
    """Полная информация о метро: мета + линии + список станций."""
    return {
        **METRO_INFO,
        "lines": METRO_LINES,
        "stations": METRO_STATIONS,
    }


def get_stations(
    line_filter: str | None = None,
    district_filter: str | None = None,
) -> list[dict]:
    """Список станций с опциональным фильтром по линии или району."""
    stations = METRO_STATIONS
    if line_filter:
        stations = [s for s in stations if s["line"] == line_filter]
    if district_filter:
        df = district_filter.lower()
        stations = [s for s in stations if df in s["district"].lower()]
    return stations
