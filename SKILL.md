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
| `api.py` | FastAPI: app core, CORS, startup events, router includes, `/run-tests` SSE |
| `routes/` | APIRouter-модули: data, ecology, transport, cameras, medical, twogis, ciinsu, studio, admin |
| `updater.py` | Тихое обновление без CLI-рендера: lazy load + background preloader |
| `registry.py` | Загрузка `config/datasets.yaml` |
| `fetcher.py` | HTTP-загрузка CSV с TTL |
| `parser.py` | Нормализация CSV (encoding, delimiter) |
| `cache.py` | DuckDB: хранение opendata-тем (таблицы `topic_*`) |

### Модули routes/ (APIRouter)

| Файл | Эндпойнты | Описание |
|---|---|---|
| `routes/data.py` | `/ask`, `/topics`, `/update`, `/power/update` | Главный запрос, темы, обновление |
| `routes/ecology.py` | `/ecology/*`, `/life-indices` | Экология, ПДК, индексы жизни |
| `routes/transport.py` | `/transit*`, `/traffic-index`, `/yandex-traffic` | Транспорт, пробки |
| `routes/cameras.py` | `/cameras*` | Камеры видеофиксации |
| `routes/medical.py` | `/medical*` | Медучреждения |
| `routes/twogis.py` | `/twogis/*`, `/mapgl-key` | 2GIS интеграция, геокодер |
| `routes/ciinsu.py` | `/ciinsu/*`, `/news-editor` | ЦИИ НГУ, редактор новостей |
| `routes/studio.py` | `/studio/*` | Data Studio для аналитиков |
| `routes/admin.py` | `/admin/*`, `/api/city-config`, `/dev-auth`, `/api/set-city` | Администрирование, регламенты |

> `api.py` (~900 строк) — app core: создание FastAPI, CORS, `.env` loader, 5 startup events, навигационная панель Swagger UI, `/run-tests` SSE, `include_router()` для всех 9 модулей.

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
  api.py              — FastAPI app core (~900 строк)
  routes/             — APIRouter-модули (9 файлов, см. таблицу выше)
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
4. Добавить рендер в соответствующий `routes/*.py` (и `renderer.py` для CLI)

---

## Карта 2GIS mapgl — паттерны и ловушки

> Этот раздел документирует нетривиальные решения, найденные в проекте. Воспроизводить при добавлении новых карт.

### 1. Кликабельные маркеры: SDK-события, не DOM

**Проблема:** `markerEl.addEventListener('click', ...)` никогда не срабатывает — mapgl canvas перехватывает pointer-события до DOM-элементов.

**Решение:** использовать SDK-события:
```javascript
const marker = new mapgl.Marker(mapInstance, { coordinates, element: markerEl });
marker.on('click', (e) => { /* e.originalEvent = нативный MouseEvent */ });
```

**Закрыть попап по клику на карту:**
```javascript
mapInstance.on('click', () => {
  if (!_markerJustClicked) _closePopup();
});
```

**Проблема двойного срабатывания:** `marker.on('click')` и `map.on('click')` оба срабатывают на один клик. Решение — флаг с задержкой:
```javascript
marker.on('click', (e) => {
  _markerJustClicked = true;
  setTimeout(() => { _markerJustClicked = false; }, 50);
  // ... показать попап
});
```

### 2. Позиционирование fixed-попапа: десктоп + touch

На **десктопе** `e.originalEvent` — `MouseEvent` с `.clientX/.clientY`.
На **мобильном** `e.originalEvent` — `TouchEvent` без `.clientX` (оно `undefined`).

Используется вспомогательная функция `_getEventCoords(e)` — единственное место, знающее о touch. `_showPopup` не изменялась:

```javascript
function _getEventCoords(e) {
  const oe = e.originalEvent;
  if (oe && oe.changedTouches && oe.changedTouches.length > 0) {
    // Touch-устройство (iOS/Android)
    return { x: oe.changedTouches[0].clientX, y: oe.changedTouches[0].clientY };
  }
  if (oe && oe.clientX != null) {
    // Десктоп — MouseEvent (основной путь, логика не изменена)
    return { x: oe.clientX, y: oe.clientY };
  }
  // Финальный fallback: SDK e.point — пиксели внутри canvas
  const rect = document.getElementById('map2gis').getBoundingClientRect();
  return { x: rect.left + e.point.x, y: rect.top + e.point.y };
}

// В обработчике marker.on('click'):
const { x: cX, y: cY } = _getEventCoords(e);
_showPopup(row, markerEl, cX, cY);
// Попап с position:fixed → cX+16, cY-28
```

**Правило:** при добавлении новых обработчиков маркеров всегда использовать `_getEventCoords(e)`, не читать `e.originalEvent.clientX` напрямую.

### 3. Контент тултипа: ветки по типу датасета (`_buildMarkerLabel`)

Определять тип строки нужно по **присутствию уникального поля** (`'field' in row`), а не по truthy-значению. Это позволяет обработать строки с пустыми значениями полей корректно.

| Датасет | Уникальное поле-ключ | Приоритетный контент тултипа |
|---|---|---|
| Камеры (OSM) | `row.osm_id !== undefined` | ⚡ скорость + название дороги |
| Школы / детсады | `row.OuName` (truthy) | Тип + № (Школа №42) + адрес |
| Строительство | `row.NameOb` (truthy) | Тип объекта + адрес + застройщик |
| Остановки | `'OstName' in row` | Название + маршруты А/Т/МТ |
| Библиотеки | `'BiblName' in row` | Тип (Детская/Научная) + имя + адрес |
| Аптеки | `'Name' in row` | Спецтип (если ≠ "Аптека") + имя + адрес |
| Спортклубы | `'NazvUch' in row` | VidSporta + название + адрес |
| Спортплощадки | `'VidSport' in row` | Тип объекта + VidSport + адрес |
| Культура | `'ShortName' in row` | ShortName + Ulica/Dom |
| Парковки / прочее | fallback | ParkType + AdrStreet |

**Порядок веток важен:** камеры и школы проверяются первыми (truthy-check), специфичные датасеты — через `'field' in row`.

### 4. Парсинг маршрутов остановок (`_formatRoutes`)

Поле `Marshryt` имеет формат: `"Автобус: 6, 18, 44.Троллейбус: 10, 22.Маршрутное такси: 44а."`.

**Алгоритм:** split по точке перед заглавной буквой → парсить каждую секцию по `:`
```javascript
const sections = s.split(/\.\s*(?=[А-ЯA-Z])/);
// Каждая секция: typePart = "Автобус", numPart = "6, 18, 44"
```

Коды транспорта: `А` (автобус), `Т` (троллейбус), `МТ` (маршрутное такси), `Тр` (трамвай), `М` (метро).

### 5. Извлечение краткого названия

**Школы / детсады (`_shortOuName`):** длинное официальное имя → `Тип № N`
```javascript
// "МБОУ Средняя общеобразовательная школа №42" → "Школа №42"
// Паттерны: /детск[а-яё]*[-\s]+сад/, /гимназ/, /лице/, /школ/
// Номер из: s.match(/[№#]\s*(\d+)/) → "№42"
// Имя из:   s.match(/«([^»]{1,20})»/) → "«Горностай»"
```

**Строительные объекты (`_shortNameOb`):** длинное юридическое описание → тип объекта
```javascript
// "Строительство многоквартирного жилого дома..." → "Многоквартирный дом"
// ~20 паттернов: МКД, апартаменты, склад, АЗС, поликлиника и т.д.
```

### 6. Тип библиотеки (`_libTypeLabel`)

```javascript
// Из BiblName детектировать тип по стемам:
// детск|ЦДБ → "Детская"  (индиго)
// научн      → "Научная"
// юношеск    → "Юношеская"
// центральн|ЦГБ|ЦБ → "Центральная"
// иначе → '' (не показываем лейбл — BiblName уже информативен)
```

---

## JS: ловушки при работе с кириллицей в регекспах

### `\b` не работает с кириллицей

В JavaScript `\b` — граница слова между `[A-Za-z0-9_]` и остальными символами. Кириллица — всегда «не-слово», поэтому `\bя\b` **никогда не совпадает** с «я» в русском тексте.

**Вместо `\b` использовать `(?:^|\s)` и `(?:\s|$)`:**
```javascript
// НЕВЕРНО — никогда не сработает:
/\bя\s+(горожанин|мэр)\b/i

// ВЕРНО — позиционная привязка:
/(?:^|\s)я\s+(?:горожанин|мэр)(?:\s|$)/i
```

### Переключение ролей голосом (`_trySpecialQuery`)

После срезки wake-word «Сигма» фраза должна **начинаться** с ключевого слова (якорь `^`). «Я» — необязательно:
```javascript
// Горожанин:  "горожанин", "я горожанин", "Сигма, горожанин"
/^(?:я\s+)?(?:горожанин[а-яё]*|жительниц[аеу]?|житель[яю]?|…)(?:\s|$)/i

// Служащий:   "служащий [район]", "я служащий Академгородок"
/^(?:я\s+)?служащ/i   // + _extractDistrict(ql) для района

// Мэр:        "мэр", "руководитель", "Сигма! Мэр"
/^(?:я\s+)?(?:мэр|руководитель)(?:\s|$)/i
```

`_extractDistrict(text)` ищет стемы районов (label.slice(0, -2)) в строке — работает для всех падежей («советского», «академгородок», «кольцово»).

Wake-word стриппинг в voice handler: `/^(с[ие]гм[ао][!,.]?\s*)/i` — охватывает «Сигма», «Сима», «Сегма» (частые ошибки распознавания, буква «Г» иногда «съедается»). В `_trySpecialQuery` — аналогичный strip `/^с[ие]гм[ао][!,.:)?\s]*/i` как страховка.

**Правило:** при расширении списка wake-word менять оба регекспа одновременно.

---

## Адаптивный интерфейс: десктоп / мобильный

> Весь адаптив реализован через Tailwind responsive-префиксы (`sm:` = ≥ 640 px). Не использовать медиазапросы в `<style>` — только классы.

### Шапка (header): принцип «flex-wrap»

Брендинговая строка использует `flex flex-wrap` вместо `flex nowrap`. Два независимых блока:

```html
<!-- Левый: котик + название — всегда в первой строке -->
<div class="flex items-center gap-3 min-w-0"> ... </div>

<!-- Правый: роль + бейджи — на десктопе рядом, на мобильном переносится на строку ниже -->
<div class="flex items-center gap-1.5 sm:gap-2 flex-shrink-0 w-full sm:w-auto justify-end">
  <!-- w-full justify-end → кнопка роли у правого края на мобильном -->
</div>
```

`min-w-0` на flex-детях — обязательно, иначе текст вызывает overflow и разрушает layout.

### Элементы с разным видом на мобильном / десктопе

| Элемент | Мобильный | Десктоп |
|---|---|---|
| Котик | `h-14` (56 px) | `h-20 sm:h-20` (80 px) |
| Заголовок «Сигма» | `text-2xl` | `sm:text-3xl` |
| Подзаголовок | `text-xs` | `sm:text-sm` |
| Кнопка «Спросить» | только `→` (`hidden sm:inline` для текста) | «Спросить →» |
| Кнопка микрофона | `w-12` (48 px фиксированная) | `sm:w-auto sm:px-4` |
| Значок 2GIS | 📍 (без текста, `hidden sm:inline`) | 📍 2GIS |
| API Docs | скрыт (`hidden sm:flex`) | показан |

### Выпадающее меню роли на мобильном

Дропдаун позиционирован `right: 0` — открывается влево от кнопки. Чтобы кнопка была у правого края экрана на мобильном, контейнер правых кнопок получает `w-full justify-end`.

Дополнительно: `max-width: calc(100vw - 16px)` — дропдаун не уходит за левый край даже на узких экранах (320 px).

### PWA (добавление на домашний экран)

Файлы:
- `src/static/manifest.json` — метаданные PWA (name, icons, theme_color `#2563eb`, background `#1e3a5f`)
- `src/static/img/favicon.svg` — **чистый векторный SVG** (без `<image href>`), котик нарисован через `polygon/ellipse/path`. SVG-фавиконы с внешними `href` не работают в браузерах из соображений безопасности.
- `src/static/img/sigma-cat.png` — PNG 1408×768 с удалённым чёрным фоном (flood-fill, threshold=30), используется в шапке и как maskable-иконка 512×512 в манифесте.

`<link rel="manifest">`, `<link rel="icon" type="image/svg+xml">`, `<link rel="apple-touch-icon">` — в `<head>` index.html.

---

## Логирование неотвеченных запросов (Railway)

В `routes/data.py` (без Volume, только stdout):
```python
import logging
log = logging.getLogger(__name__)
# ...
log.info("UNKNOWN_QUERY: %s", q)      # перед return {"operation": "UNKNOWN"}
```
Railway Dashboard → Logs tab → фильтр `UNKNOWN_QUERY`. Без персистентного диска, данные живут пока жив деплой.

---

## Мульти-город архитектура

> **Принцип:** `config/city_profile.yaml` — единственный источник городского знания. Все Python-модули читают город только через `src/city_config.py`. Прямое чтение YAML в других модулях запрещено.

### Переключение города — серверный режим (env)

```bash
CITY_PROFILE=city_profile_omsk bot serve       # Linux/macOS
$env:CITY_PROFILE="city_profile_omsk"; bot serve  # Windows PowerShell
```

| Env | Файл | Поведение |
|---|---|---|
| не задан | `config/city_profile.yaml` | Новосибирск (дефолт) |
| `city_profile_omsk` | `config/city_profile_omsk.yaml` | Омск |
| абсолютный путь | `/etc/mybot/omsk.yaml` | произвольный путь |

### Переключение города — UI (runtime, без перезапуска)

Кнопка «🏙 Город ▾» в шапке index.html вызывает `_toggleCityMenu()` → показывает dropdown из `_availableCities`.

**API:**
- `GET /api/available-cities` → `{cities: [{city_id, city_name}, ...]}` (все `city_profile_*.yaml` в `config/`)
- `POST /api/set-city {city_id}` → устанавливает `os.environ["CITY_PROFILE"]`, сбрасывает `lru_cache`, обновляет профиль
- `GET /api/city-config` → возвращает `_cityCfg` для фронта (city_id, districts, has_opendata_csv, static_datasets...)

**Фронт (index.html):**
```javascript
_loadAvailableCities()   // при инициализации — GET /api/available-cities
_setActiveCity(cityId)   // POST /api/set-city → loadCityConfig() → loadTopics()
```

`loadCityConfig()` устанавливает `_cityHasOpendataCsv = !!cfg.has_opendata_csv` и вызывает `loadTopics()`.

### Фильтрация CSV-карточек по городу

В `renderTopics()` (index.html) CSV-плашки фильтруются двумя условиями:
```javascript
topics.filter(t => _cityHasOpendataCsv && t.rows != null && t.rows > 0)
```
- `_cityHasOpendataCsv = false` для городов без `opendata_csv_enabled: true` в профиле (Омск и др.)
- Важно: backend `cache.py` хранит `_DB_PATH` на уровне модуля (устанавливается при импорте = NSK-путь), поэтому `/topics` может вернуть NSK-строки даже после переключения города. Фронтовый фильтр — надёжная защита.

### Омск: специфика

- **Отключения ЖКХ:** `power_scraper.py` диспетчеризует по `city_id == "omsk"` → `power_scraper_omsk.py` (POST API `omskrts.ru`, парсит адреса + группирует по типу ресурса / периоду)
- **CSV-темы:** не подключены (`opendata_csv_enabled` не задан), плашки скрыты
- **Аэропорт, Экология, Камеры, Медицина:** работают через Omsk-координаты из профиля

### Ключевые функции city_config.py

| Функция | Возвращает |
|---|---|
| `get_city_id()` | `"novosibirsk"` |
| `get_city_name(case)` | `"Новосибирск"` / `"Новосибирска"` / `"Новосибирске"` |
| `get_districts()` | `{"Советский район": ["советск"], ...}` |
| `get_district_coords()` | `{"Советский район": (83.1091, 54.8441), ...}` |
| `get_sub_districts_compiled()` | список tuple `(re.Pattern, "Район", "Подрайон")` |
| `get_ecology_stations()` | список `{station_id, district, lat, lon, ...}` |
| `get_dataset_path(name)` | `Path` к файлу данных или `None` если `enabled: false` |
| `get_metro_path()` | `Path` к `metro.json` или `None` |
| `get_airport_path()` | `Path` к `airport.json` или `None` |

**lru_cache:** профиль загружается **один раз**. При тестах со сменой профиля: `get_city_profile.cache_clear()`.

### Graceful degradation

Если датасет `enabled: false` в `city_profile.yaml`:
- `get_dataset_path(name)` → `None`
- загрузчик бросает `FileNotFoundError` с понятным сообщением
- бот возвращает ответ «данные недоступны для этого города» (не 500)

### Структура city_profile.yaml

```
city:             id, name, name_genitive, name_prepositional, slug, timezone, utc_offset, center, bbox
districts:        канон. название → [стемы]
sub_districts:    [{name, parent, patterns, examples}]
district_coords:  канон. название → {lon, lat}
city_stopwords:   слова-исключения для геокодера
ecology_stations: [{station_id, district, address, lat, lon}]
features:         has_metro, has_airport, airport_iata, power_outages_url, ...
opendata_base_url
static_datasets:  emissions, heat_sources, metro, airport → {enabled, file, ...}
```

---

## Статические датасеты (JSON/GeoJSON)

Загружаются один раз через `lru_cache`. Путь из `city_profile.yaml → static_datasets.<name>.file`.

| Датасет | Файл | Формат | Загрузчик |
|---|---|---|---|
| `emissions` | `data/nsk_emissions_2tp.json` | JSON → `{"municipalities":[...]}` | `src/emissions.py` |
| `heat_sources` | `data/nsk_heat_sources_v1.geojson` | GeoJSON FeatureCollection | `src/heat_sources.py` |
| `metro` | `data/cities/novosibirsk/metro.json` | JSON → `{"info":{}, "lines":{}, "stations":[...]}` | `src/metro_data.py` |
| `airport` | `data/cities/novosibirsk/airport.json` | JSON → `{name, iata, terminals:[], transport:[]}` | `src/airport_data.py` |

### Добавить датасет для нового города

1. Создать файл в `data/cities/<city_id>/` (структуру смотреть в `config/canonical_schemas.yaml`)
2. В `city_profile_<id>.yaml` → `static_datasets.<name>: {enabled: true, file: "data/cities/<city_id>/..."}`
3. Перезапустить — `lru_cache` сбросится автоматически

---

## Канонические схемы (canonical_schemas.yaml)

`config/canonical_schemas.yaml` — единственный источник правды об именах полей.

**Правило: имена полей нельзя менять** без обновления кода в `executor.py`, `renderer.py`, `heat_sources.py`, `emissions.py` и тестов.

| Соглашение | Пример |
|---|---|
| Строчные, транслит, подчёркивание | `vid_sporta`, `nazvanie_polnoe` |
| Координаты всегда с `_` | `_lat`, `_lon` |
| Единицы в имени | `vsego_t` (тонны), `thermal_gcal_h` (Гкал/ч), `mest_kol` (количество) |
| Район в CSV-темах | `rayon` |
| Район в статических | `district` |

| Секция | Что содержит |
|---|---|
| `csv_topics` | 10 тем opendata: parking, stops, schools, ... (поля для executor.py SQL) |
| `static_datasets` | emissions, heat_sources, metro_station, metro_line, airport, terminal, transport |
| `config_structures` | ecology_station, district_coord (для city_profile.yaml) |

Просмотр: `GET /studio/api/schemas`.

---

## Docker-деплой (сервер мэрии)

| Файл | Назначение |
|---|---|
| `Dockerfile` | python:3.11-slim, `pip install -e .`, `bot serve --host 0.0.0.0` |
| `docker-compose.yml` | bot + (опц.) nginx; `data/` как Volume; healthcheck на `/topics` |
| `deploy/nginx.conf` | HTTP→HTTPS redirect, proxy → localhost:8000, SSL через certbot |
| `.env.example` | шаблон: `CITY_PROFILE`, `TWOGIS_API_KEY`, `CITYAIR_API_KEY` |

```bash
git clone <repo> /opt/city-bot && cd /opt/city-bot
cp .env.example .env && nano .env
docker-compose up -d
curl http://localhost:8000/topics
```

**Обновление данных:** `docker-compose restart bot` (без пересборки образа).
**Обновление кода:** `git pull && docker-compose build --no-cache && docker-compose up -d`.

| | Минимум | Рекомендуется |
|---|---|---|
| CPU | 1 ядро | 2 ядра |
| RAM | 512 МБ | 2 ГБ |
| Диск | 10 ГБ | 50 ГБ |
| ОС | Ubuntu 20.04 | Ubuntu 22.04 LTS |

---

## Вход для разработчиков и аналитиков регламентов

Кнопка **«Вход для разработчиков 🔒»** в подвале `index.html` → модальный диалог ввода пароля → сессия сохраняется в `sessionStorage('dev_auth_ok')`.

Эндпоинт аутентификации (`routes/admin.py`): `GET /dev-auth?password=<value>` → `{valid: true/false}`.
Пароль по умолчанию: `sigma2024`. Хранится как SHA-256 в `data/api_keys.json`.

После входа доступны два интерфейса:

| Интерфейс | URL | Для кого |
|---|---|---|
| API Документация | `/docs` | Разработчики — Swagger с тест-запросами |
| Data Studio | `/studio` | Аналитики регламентов — города, импорт данных, схемы |

## Data Studio (/studio)

Визуальный интерфейс управления городскими данными. Для аналитиков регламентов городской среды.

| Вкладка | Что делает |
|---|---|
| **Города** | Список всех `city_profile*.yaml`, статус датасетов (включён / файл есть / отсутствует) |
| **Онлайн-источники** | URL порталов открытых данных и ЖКХ-отключений — редактируются без перезапуска |
| **Импорт данных** | Загрузить CSV/JSON → предпросмотр → маппинг колонок → сохранить |
| **Справочник схем** | Канонические схемы из `canonical_schemas.yaml` |

### Studio API (внутренние, не в Swagger)

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/studio` | HTML-страница Studio |
| `GET` | `/studio/api/profiles` | Все city profiles + статус датасетов |
| `GET` | `/studio/api/schemas` | Canonical schemas (YAML → JSON) |
| `POST` | `/studio/api/preview` | Загрузить файл → `{columns, sample[5]}` |
| `POST` | `/studio/api/import` | Загрузить + маппинг → сохранить в `data/cities/<city_id>/` |
| `GET` | `/api/city-config` | Текущий city_profile в виде JSON для фронта |
| `GET` | `/api/available-cities` | Список всех доступных городов |
| `POST` | `/api/set-city` | Переключить активный город (runtime, без перезапуска) |
| `GET` | `/api/online-sources/<city_id>` | Онлайн-источники для редактирования |
| `POST` | `/api/edit-source` | Сохранить URL источника в city_profile_*.yaml |

### Импорт CSV: flow

1. Выбрать город (`city_id`) и тип датасета
2. Загрузить CSV/JSON → предпросмотр 5 строк + список колонок
3. Сопоставить каждое каноническое поле с колонкой источника
4. Нажать «Импортировать» → сохраняется в `data/cities/<city_id>/<dataset_type>.json`
5. В `city_profile_<id>.yaml` вручную установить `enabled: true, file: ...`
