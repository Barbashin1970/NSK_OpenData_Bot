# NSK OpenData Bot — SKILL.md

CLI-агент и веб-платформа для работы с открытыми данными и системами жизнеобеспечения Новосибирска.
Разработан для **ЦИИ НГУ** (Центра ИИ НГУ). Демонстрирует агентный подход: «семантика + кэш + реальное время».

> **Принцип работы:** Rule-based, **без AI at runtime**. Claude используется только как dev-инструмент (читает этот файл). Вся логика — keyword-matching + SQL + скрапинг.

---

## Быстрый старт

```bash
pip install -e .
bot update --all            # загрузить все 12 тем CSV (~1–2 мин)
bot serve                   # запустить API + Web UI → http://127.0.0.1:8000
bot ask "сколько школ в Советском районе"
```

> **После деплоя данные загружаются автоматически:** lazy load при первом запросе + background preloader (15 сек задержка после старта, 5 сек между темами). Ручной `bot update` не обязателен.

---

## Архитектура

```
Запрос → router.py → planner.py → executor.py → renderer.py / api.py
```

| Модуль | Роль |
|---|---|
| `router.py` | Keyword-matching (стемы), confidence score → topic |
| `planner.py` | Regex-паттерны → операция (COUNT/GROUP/TOP_N/FILTER/POWER_*/ECO_*/…) |
| `executor.py` | SQL к DuckDB / scrape / external API |
| `renderer.py` | Rich-вывод в терминале |
| `api.py` | FastAPI: все HTTP-эндпойнты, startup-хук preloader |
| `updater.py` | Тихое обновление без CLI-рендера: lazy load + background preloader |
| `registry.py` | Загрузка `config/datasets.yaml` |
| `fetcher.py` | HTTP-загрузка CSV с TTL |
| `parser.py` | Нормализация CSV (encoding, delimiter) |
| `cache.py` | DuckDB: хранение opendata-тем (таблицы `topic_*`) |

---

## Поддерживаемые темы и источники данных

### 12 тем открытых данных (opendata.novo-sibirsk.ru, TTL 24 ч)

| topic_key | Название | Кол-во | Ключевые поля в ответе |
|---|---|---|---|
| `parking` | Парковки | ~2360 | AdrDistr, AdrStreet, ParkType, NumMashMest, ParkOhrana, Regim |
| `stops` | Остановки ОТ | ~746 | AdrDistr, OstName, Pavilion, Marshryt |
| `schools` | Школы | ~214 | OuName, **RukName** (директор), AdrDistr, AdrStreet, Phone, Regimrab |
| `kindergartens` | Детские сады | ~253 | OuName, AdrDistr, AdrStreet, Mesta, Phone |
| `libraries` | Библиотеки | ~11 | BiblName, BiblFName, AdrDistr, AdrStreet, Phone, Site |
| `culture` | Культура | ~11 | ShortName, FullName, Rayon, Ulica, TelUch, Site |
| `parks` | Парки | — | AdrDistr, AdrStreet, ParkName |
| `sport_grounds` | Спортплощадки | ~142 | District, Street, Type, VidSport, Phone |
| `pharmacies` | Аптеки | ~27 | Name, AdrDistr, AdrStr, Phone |
| `sport_orgs` | Спортклубы | ~89 | NazvUch, Rayon, Ulica, VidSporta, TelUch |
| `construction_permits` | Разрешения на строительство | ~5942 | NomRazr, DatRazr, Zastr, NameOb, AdrOr, KadNom |
| `construction_commissioned` | Ввод в эксплуатацию | ~1935 | NomRazr, DatRazr, Zastr, NameOb, Raion, AdrOb, KadNom |

> **Строительство** — особая логика: `construction_permits` − `construction_commissioned` (set-diff по KadNom) = активные стройки. Реализовано в `construction_opendata.py`.

### Специальные темы реального времени

| topic | Источник | TTL | Что даёт |
|---|---|---|---|
| `power_outages` | 051.novo-sibirsk.ru (скрапинг) | 30 мин | Отключения электро/тепло/вода/газ, история 7 дней |
| `ecology` | Open-Meteo + (opt.) CityAir | 15 мин | AQI, PM2.5, PM10, NO2, погода, прогноз |
| `cameras` | OpenStreetMap Overpass API | 7 дней | ~60 камер фиксации нарушений ПДД, координаты |
| `traffic_index` | Синтетическая модель (7 факторов) | real-time | Индекс пробок 0–10, советы гражданину/чиновнику |
| `transit` | 2GIS Routing API (pass-through) | real-time | Маршрут ОТ между районами, без хранения |

---

## CLI-команды

### `bot ask "<запрос>"`

```bash
# Подсчёт
bot ask "сколько школ в Новосибирске"
bot ask "сколько аптек в Советском районе"

# Группировка по районам
bot ask "парковки по районам"
bot ask "детские сады по районам"

# Топ-N
bot ask "топ-10 парковок по числу мест"
bot ask "топ-5 библиотек"

# Список с фильтром
bot ask "покажи библиотеки в Ленинском районе"
bot ask "аптеки на улице Ленина"
bot ask "школы в Академгородке"

# Строительство
bot ask "активные стройки в Центральном районе"
bot ask "разрешения на строительство в 2024"
bot ask "что введено в эксплуатацию в Советском районе"

# Отключения ЖКХ (все типы)
bot ask "отключения электричества сегодня"
bot ask "есть ли горячая вода в Дзержинском районе"
bot ask "плановые отключения теплоснабжения на этой неделе"
bot ask "история отключений газа за 7 дней"
bot ask "все коммунальные отключения сейчас"

# Экология
bot ask "качество воздуха в Новосибирске"
bot ask "где сейчас самый загрязнённый воздух"
bot ask "превышение ПДК по PM2.5 сегодня"
bot ask "прогноз качества воздуха на завтра"
bot ask "динамика PM2.5 за неделю"

# Транспорт
bot ask "как добраться из Советского района в Центральный"
bot ask "пробки сейчас"
bot ask "индекс пробок"
bot ask "камеры видеофиксации в Ленинском районе"
```

### `bot update`

```bash
bot update --all                          # все 12 тем (~1–2 мин)
bot update --topic schools                # конкретная тема
bot update --topic construction_permits   # строительство (датасет 124)
bot update --topic construction_commissioned  # датасет 125
bot update --topic parking --force        # принудительно, игнорировать TTL
```

### `bot power`

```bash
bot power update                          # обновить отключения ЖКХ
bot power status                          # электроснабжение: сейчас
bot power status --all-utilities          # все типы: вода, газ, тепло, электро
bot power status --district "Советский район"
bot power planned                         # запланировано на сегодня
bot power history                         # история 7 дней
bot power history --days 3
```

### `bot ecology`

```bash
bot ecology update
bot ecology status                        # AQI, PM2.5 по всем 10 районам
bot ecology status --district "Советский район"
bot ecology pdk                           # превышения ПДК WHO (PM2.5 > 35 мкг/м³)
bot ecology history                       # динамика 7 дней
bot ecology history --days 3
```

### `bot topics` и `bot serve`

```bash
bot topics          # статус кэша всех тем
bot serve           # HTTP API → http://127.0.0.1:8000
```

---

## HTTP API (FastAPI)

Запуск: `bot serve` → http://127.0.0.1:8000. Swagger UI: `/docs`.

### Основные эндпойнты

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/ask?q=...&with_coords=true` | Главный запрос. `with_coords=true` обогащает строки координатами через 2GIS (max 50 строк, FILTER/TOP_N) |
| `GET` | `/topics` | Список тем: статус кэша, кол-во строк, last_updated |
| `POST` | `/update?topic=<id>` | Обновить одну тему или все 12 (без topic). Доступные ID: все 12 из таблицы выше |
| `POST` | `/power/update` | Обновить отключения ЖКХ |
| `POST` | `/ecology/update` | Обновить экологию |
| `POST` | `/cameras/update` | Обновить камеры (Overpass API, TTL 7 дней) |
| `GET` | `/ecology/current` | Текущие измерения AQI/PM |
| `GET` | `/ecology/forecast?days=7` | Прогноз погоды |
| `GET` | `/ecology/history?days=30` | История экологии |
| `GET` | `/transit?from=...&to=...` | Маршрут ОТ между районами (pass-through 2GIS, без хранения) |
| `GET` | `/transit-districts` | Кол-во остановок по районам |

### 2GIS интеграция

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/twogis/key` | Текущий ключ (замаскированный) + источник |
| `POST` | `/twogis/key?key=...` | Сохранить ключ в `data/api_keys.json` |
| `GET` | `/twogis/validate?key=...` | Проверить ключ через Geocoder API |
| `GET` | `/twogis/geocode?q=...&city=...` | Геокодировать адрес (кэшируется в DuckDB) |
| `GET` | `/twogis/geocache-stats` | Статистика кэша геокодирования |

### ЦИИ НГУ

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/ciinsu/<section>` | Секции: `center`, `projects`, `team`, `publications`, `news`, `contacts`, `all` |
| `POST` | `/ciinsu/login` | Аутентификация (пароль → токен) |
| `POST` | `/ciinsu/news` | Добавить новость (требует токен) |
| `PUT` | `/ciinsu/news/<id>` | Редактировать новость |
| `DELETE` | `/ciinsu/news/<id>` | Удалить новость |
| `GET` | `/news-editor` | Web-редактор новостей |

### Служебные

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/` | Главная страница (Web UI) |
| `GET` | `/docs` | Swagger UI с тестовой панелью |
| `GET` | `/run-tests` | SSE-стрим: pytest + health check |

---

## Типы операций (planner.py)

### Стандартные (все 12 opendata-тем)

| Ключевые слова | Операция | SQL |
|---|---|---|
| «сколько», «количество», «число» | `COUNT` | `SELECT COUNT(*)` |
| «по районам», «по каждому», «где больше» | `GROUP` | `GROUP BY district_col` |
| «топ-N», «первые N», «наибольших» | `TOP_N` | `ORDER BY count_col LIMIT N` |
| «покажи», «список», «найди», «все» | `FILTER` | `SELECT display_cols WHERE ...` |

### Строительство

| Операция | Смысл |
|---|---|
| `CONSTRUCTION_ACTIVE` | permits − commissioned (set-diff по KadNom) |
| `CONSTRUCTION_PERMITS` | все разрешения (датасет 124) |
| `CONSTRUCTION_COMMISSIONED` | все введённые (датасет 125) |
| `CONSTRUCTION_COUNT` | COUNT по типу разрешения |
| `CONSTRUCTION_GROUP` | GROUP по районам |

### Отключения ЖКХ (power_outages)

| Операция | Смысл |
|---|---|
| `POWER_STATUS` | Активные прямо сейчас |
| `POWER_TODAY` | Все за сегодня |
| `POWER_PLANNED` | Запланированные |
| `POWER_HISTORY` | История по дням (до 7 дней) |

**Типы ресурсов** (`extra_filters["utility"]`):

| Что пишет пользователь | ILIKE-фильтр |
|---|---|
| электричество, свет, электро | `электроснабж` |
| тепло, отопление | `теплоснабж` |
| горячая вода | `горяч` |
| холодная вода | `холодн` |
| газ | `газоснабж` |
| все / ЖКХ / коммунальные | `""` (пустой = все типы) |

### Экология

| Операция | Смысл |
|---|---|
| `ECO_STATUS` | Текущий AQI, PM2.5, погода по 10 районам |
| `ECO_PDK` | Превышения ПДК ВОЗ (PM2.5 > 35 мкг/м³) |
| `ECO_HISTORY` | Динамика по дням (7 дней) |
| `ECO_FORECAST` | Прогноз (Open-Meteo, проверяется ПЕРВЫМ перед HISTORY) |
| `ECO_RISKS` | Риски: гололедица, НМУ, индекс |

### Специальные

| Операция | Тема | Смысл |
|---|---|---|
| `TRANSIT_ROUTE` | transit | Маршрут ОТ от района до района |
| `TRAFFIC_INDEX` | traffic_index | Синтетический индекс пробок 0.0–10.0 |
| `CAMERAS` | cameras | Список камер ПДД с фильтром по районам |

---

## Районы Новосибирска

10 административных районов:

| В запросе | Канонический SQL-фильтр |
|---|---|
| «дзержинском», «дзержинский» | Дзержинский район |
| «железнодорожном», «железнодорожный» | Железнодорожный район |
| «заельцовском», «заельцов» | Заельцовский район |
| «калининском», «калининский» | Калининский район |
| «кировском», «кировский» | Кировский район |
| «ленинском», «ленинский» | Ленинский район |
| «октябрьском», «октябрьский» | Октябрьский район |
| «первомайском», «первомайский» | Первомайский район |
| «советском», «советский» | Советский район |
| «центральном», «центральный», «в центре» | Центральный район |

### Подрайоны (Советский район)

Маппируются на Советский район с сохранением `plan.sub_district` для отображения:

| Что пишет пользователь | Подрайон | Родитель |
|---|---|---|
| «в Академгородке», «Академгородок», «Academ», «в Академе» | Академгородок | Советский |
| «на Шлюзе», «в Шлюзе», «Шлюз» | Шлюз | Советский |
| «в Верхней зоне», «Верхняя зона» | Верхняя зона | Советский |
| «мкр. Щ», «микрорайон Щ», «в Щ», просто «Щ» | мкр. "Щ" | Советский |

> Добавить подрайон: `src/router.py` → список `_SUB_DISTRICTS` → tuple `(re.compile(pattern), "Район", "Название")`.

---

## Автообновление данных (updater.py)

Два механизма работают без ручного вмешательства разработчика:

### 1. Lazy load (при запросе пользователя)

Если таблица не существует → синхронная загрузка нужной темы (3–8 сек), потом кэш на 24 ч.

- Стандартные темы: `ensure_fresh(topic)` → `refresh_topic()` → fetch CSV → load DuckDB
- Строительство: `ensure_fresh("construction_permits")` + `ensure_fresh("construction_commissioned")`

### 2. Background preloader (при старте сервера)

Запускается через `asyncio.create_task()` в `@app.on_event("startup")`.

- Задержка старта: **15 сек** (Railway успевает пройти health check)
- Пауза между темами: **5 сек** (не нагружает сервер)
- Порядок загрузки (от популярных к редким):
  1. stops → 2. schools → 3. kindergartens → 4. pharmacies → 5. libraries
  6. parking → 7. sport_grounds → 8. sport_orgs → 9. culture
  10. construction_permits → 11. construction_commissioned
- Пропускает темы, у которых TTL не истёк

---

## 2GIS интеграция

**Три сервиса:**

| Сервис | Кэш | Файл |
|---|---|---|
| Geocoder (адрес → lat/lon) | DuckDB `geocode_cache`, бессрочно | `geocoder.py` |
| Public Transport Routing | Нет (pass-through) | `transport_api.py` |
| Catalog API (остановки рядом) | Нет | `transport_api.py` |

**Ключ:** `ENV TWOGIS_API_KEY` (приоритет) или `data/api_keys.json`.

**Graceful degradation:** все 2GIS-функции возвращают `None`/`available:false` без ошибок, если ключ не задан.

**Правила лицензии 2ГИС:**
- Геокодирование: кэширование РАЗРЕШЕНО
- Routing/Transit: хранение ЗАПРЕЩЕНО (только pass-through)
- Трафик: хранение ЗАПРЕЩЕНО

---

## Синтетический индекс пробок (traffic_index.py)

Шкала 0.0–10.0 на основе 7 факторов:

1. **Время суток** — базовый балл (07:30–09:30 = 5.5, 16:30–19:00 = 5.0, ночь = 0.3)
2. **Выходные/праздники** — × 0.45
3. **День недели** — понедельник утром +0.8, пятница вечером +0.7
4. **Годовые события** — 1 сентября +2.0, 28–30 декабря +1.5–2.0
5. **Погода** — снегопад +1.5–2.5, ледяной дождь +2.5, дождь +0.8–1.5
6. **Гололедица** (−3…+2°C + осадки) — +1.0
7. **Сильный мороз** (< −20°C) — −0.5…−1.5 (люди переходят на ОТ)

Источник погоды: Open-Meteo прогноз + `fact_measurements` из ecology_cache. Часовой пояс: UTC+7.

---

## ЦИИ НГУ (ciinsu.py)

**Хранилище:**

| Файл | Хранение | Что |
|---|---|---|
| `src/ciinsu_knowledge_base.json` | Git (bundled) | Статика: центр, проекты, команда, публикации |
| `data/ciinsu/news.json` | Git (tracked) | Динамические новости (редактор) |
| `data/ciinsu/config.json` | .gitignore (секрет) | `token_secret`, `valid_token` |
| `data/ciinsu/photos/` | Git (tracked) | Фото к новостям |

**Аутентификация редактора:** пароль по умолчанию `sigma2024` → SHA256(password + secret) → токен.

**Секции KB:** `center`, `projects`, `team`, `publications`, `news`, `contacts`, `all`.

**Структура новости:**
```json
{
  "id": "hash8",
  "title": "...",
  "date": "YYYY-MM-DD",
  "body": "...",
  "photo": "filename.png",
  "created_at": "ISO8601"
}
```

**Workflow публикации новости:**
1. `bot serve` → браузер → `/news-editor` → логин → написать → опубликовать
2. `git add data/ciinsu/news.json data/ciinsu/photos/`
3. `git commit && git push` → Railway autodeploy

---

## Структура хранилища

```
data/
  cache.db            — DuckDB: topic_* + power_outages + geocode_cache + cameras + ecology
  meta.json           — даты обновления по темам
  api_keys.json       — TWOGIS_API_KEY (не в git)
  raw/<topic>/        — сырые CSV (не в git)
  logs/fetch.log      — лог HTTP-запросов (не в git)
  ciinsu/
    news.json         — новости (в git)
    photos/           — фото к новостям (в git)
    config.json       — токены (не в git, секрет)

src/
  ciinsu_knowledge_base.json   — KB ЦИИ НГУ (в git, bundled)
  static/
    index.html        — Web UI (SPA)

config/
  datasets.yaml       — реестр 12 тем opendata
```

---

## Деплой

| Среда | Команда / файл |
|---|---|
| Локально | `bot serve` |
| Railway | `Procfile` (git push → autodeploy) |
| macOS автозапуск | LaunchAgent plist (см. README) |

> **Railway:** контейнер ephemeral — `data/` не сохраняется между деплоями. Данные восстанавливаются автоматически через preloader. `news.json` и `photos/` живут в git → переживают деплой.

---

## Источники данных — полная таблица

| Источник | TTL | Темы | Ключ | Лицензия |
|---|---|---|---|---|
| opendata.novo-sibirsk.ru | 24 ч | 12 тем CSV | — | Открытые данные |
| 051.novo-sibirsk.ru | 30 мин | power_outages | — | Открытые данные |
| Open-Meteo Air Quality API | 15 мин | ecology (AQI, PM) | — | Бесплатно |
| Open-Meteo Forecast API | 15 мин | ecology (погода), traffic_index | — | Бесплатно |
| CityAir API | 15 мин | ecology (датчики, опц.) | `CITYAIR_API_KEY` | Платная |
| 2GIS Catalog API | real-time | geocoding, transit | `TWOGIS_API_KEY` | ODbL+Commercial |
| 2GIS Routing API | real-time | transit (pass-through) | `TWOGIS_API_KEY` | ODbL+Commercial |
| Overpass API (OSM) | 7 дней | cameras | — | ODbL |

---

## Добавление новых возможностей

### Новая тема opendata

1. Добавить блок в `config/datasets.yaml` (name, data_url, keywords, fields)
2. Добавить иконку в `TOPIC_ICONS` в `index.html`
3. Добавить тему в `PRELOAD_ORDER` в `updater.py`
4. Добавить метки колонок в `COL_LABELS` в `index.html`

### Новый подрайон

В `src/router.py` → список `_SUB_DISTRICTS`:
```python
(re.compile(r"ваш_паттерн"), "Родительский район", "Отображаемое название")
```

### Новый тип ресурса ЖКХ

В `src/router.py` → `UTILITY_FILTER_MAP` + `_detect_utility()`.

### Новая операция

1. Добавить в `Operation` enum в `planner.py`
2. Добавить паттерн в `make_plan()`
3. Добавить ветку в `executor.py`
4. Добавить рендер в `api.py` (и `renderer.py` для CLI)
