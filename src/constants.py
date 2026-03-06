"""Централизованные константы проекта NSK OpenData Bot.

Единственное место, где определяются пути к данным, параметры HTTP и TTL.
"""

from pathlib import Path

# ── Пути ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
RAW_DIR      = DATA_DIR / "raw"
LOGS_DIR     = DATA_DIR / "logs"
META_FILE    = DATA_DIR / "meta.json"
DB_FILE      = DATA_DIR / "cache.db"
CONFIG_FILE  = PROJECT_ROOT / "config" / "datasets.yaml"

# ── HTTP ──────────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "NSK-OpenData-Bot/1.0 (NSU CII; opendata.novo-sibirsk.ru)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
TIMEOUT = 30  # секунды

# ── Кэш — открытые данные ─────────────────────────────────────────────────────
DEFAULT_TTL_HOURS = 24

# ── Кэш — отключения ЖКХ ─────────────────────────────────────────────────────
POWER_TTL_MINUTES  = 30
POWER_HISTORY_DAYS = 7
POWER_FUTURE_DAYS  = 7
NSK_051_URL = "http://051.novo-sibirsk.ru/sitepages/off.aspx"

# ── Кэш — экология и метеорология ────────────────────────────────────────────
ECOLOGY_TTL_MINUTES   = 15   # Open-Meteo обновляется ~каждые 15 мин
ECOLOGY_HISTORY_DAYS  = 30   # хранить историю измерений 30 дней
ECOLOGY_LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 МБ — ротация лога

# Фиксированные точки мониторинга — по одной на каждый район Новосибирска
# Координаты соответствуют географическим центрам районов (используются Open-Meteo + CityAir)
NSK_ECOLOGY_STATIONS: list[dict] = [
    {"station_id": "nsk_central",   "district": "Центральный район",     "address": "Центр Новосибирска",    "latitude": 54.9884, "longitude": 82.9090},
    {"station_id": "nsk_soviet",    "district": "Советский район",       "address": "Академгородок",         "latitude": 54.8441, "longitude": 83.1091},
    {"station_id": "nsk_dzerzh",    "district": "Дзержинский район",     "address": "Дзержинский р-н",       "latitude": 54.9823, "longitude": 82.9650},
    {"station_id": "nsk_zhd",       "district": "Железнодорожный район", "address": "Железнодорожный р-н",   "latitude": 54.9954, "longitude": 82.8860},
    {"station_id": "nsk_zaelc",     "district": "Заельцовский район",    "address": "Заельцовский р-н",      "latitude": 54.9893, "longitude": 82.9550},
    {"station_id": "nsk_kalinin",   "district": "Калининский район",     "address": "Калининский р-н",       "latitude": 55.0190, "longitude": 82.8880},
    {"station_id": "nsk_kirov",     "district": "Кировский район",       "address": "Кировский р-н",         "latitude": 54.9600, "longitude": 82.8190},
    {"station_id": "nsk_leninsky",  "district": "Ленинский район",       "address": "Ленинский р-н",         "latitude": 54.9460, "longitude": 82.8800},
    {"station_id": "nsk_october",   "district": "Октябрьский район",     "address": "Октябрьский р-н",       "latitude": 54.9940, "longitude": 82.8190},
    {"station_id": "nsk_pervomay",  "district": "Первомайский район",    "address": "Первомайский р-н",      "latitude": 54.8780, "longitude": 82.8710},
]

# ── Scraper headers (Browser UA для 051) ─────────────────────────────────────
SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}
SCRAPER_TIMEOUT = 20  # секунды
