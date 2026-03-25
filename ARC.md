# Полная архитектура NSK OpenData Bot / Сима

## Версия 1.3.0 — март 2026

Система реализует естественно-языковой интерфейс к открытым данным городов. Поток данных однонаправленный: **запрос → классификация → планирование → исполнение → рендеринг → ответ**. Конфигурация города вынесена в горизонтальный слой CAL, который пронизывает все вертикальные слои.

***

## Изменения относительно v1.2.0

**Что сделано в v1.3.0:**

- **Volume persistence** — конфигурация, редактируемая в рантайме (регламенты, словарь), хранится в `data/` (Railway Volume), а не в `config/` (Docker image). Startup-хук `_seed_volume_data()` копирует seed-файлы при первом деплое, не перезаписывая существующие.
- **5 YAML-регламентов** — добавлен `mobile_index_rules.yaml` (транспортный индекс загрязнения) с редактором в Studio
- **Экологический отчёт за 30 дней** — интерактивный дашборд: AQI-гистограмма, температурный график, тепловая карта превышений, автокомментарии
- **AQI exceedance tracking** — эндпоинт `/ecology/aqi-exceedances` для почасового анализа паттернов загрязнения
- **Матрица подключений** в Studio теперь проверяет реальную свежесть данных (stale/fresh), а не просто наличие feature-флага
- **Storage monitoring** — `GET /admin/storage-stats` + визуальный индикатор в Studio Overview

### Изменения v1.2.0 (предыдущие)

- `api.py` стал **точкой входа** (~250 строк): создаёт `FastAPI app`, подключает middleware, startup events и регистрирует роутеры
- Вся бизнес-логика эндпоинтов перенесена в `src/routes/*.py` (9 роутер-модулей)
- SSE-стриминг тестов (`/run-tests`) остался в `api.py` как системная инфраструктура
- Swagger UI с кастомной навигацией вынесен в отдельный хендлер

***

## Слой 0 — Пользователи

| ID | Роль | Интерфейс |
| :-- | :-- | :-- |
| U1 | Горожанин | Telegram Bot / Web-чат `index.html` |
| U2 | Чиновник / Служащий | Web, параметр `role=official` |
| U3 | Аналитик данных | Data Studio SPA `studio.html` |


***

## Слой 1 — Внешние источники

| ID | Источник | Протокол | Покрытие |
| :-- | :-- | :-- | :-- |
| E1 | opendata.novo-sibirsk.ru | HTTP CSV | НСК |
| E2 | Open-Meteo (замена OpenWeatherMap) | JSON REST, бесплатно | Все города |
| E3 | 2GIS MapGL + Routes API | JS SDK + REST | НСК / Омск |
| E4 | 051.novo-sibirsk.ru (ЖКХ) | HTML scraping | НСК |
| E5 | opendata.omsk.ru | HTTP CSV | Омск |
| E6 | OpenStreetMap Overpass API | REST | Все (OSM ODbL) |
| E7 | CityAir API | JSON REST | НСК (опционально) |

**Изменение:** OpenWeatherMap заменён на Open-Meteo (бесплатный, 11 точек по районам), добавлены E6 (OSM) и E7 (CityAir) .

***

## Слой 2 — City Abstraction Layer (CAL)

**Глоссарий.** CAL — горизонтальный конфигурационный слой, декларативно описывающий город. Это единственный слой, который меняется при тиражировании системы на новый муниципалитет.


| ID | Файл | Содержимое | Связан с |
| :-- | :-- | :-- | :-- |
| C1 | `city_profile_*.yaml` | ID города, центр, bbox, UTC, районы, датасеты, feature-флаги | RF1, ET4 |
| C2 | `canonical_schemas.yaml` | Контракт имён полей — обязательные / опциональные | ET2, Data Studio |
| C3 | `rules/*.yaml` | 5 YAML-регламентов: трафик, экология, жизнь, календарь, транспортный индекс | RE |
| C4 | `datasets.yaml` | Реестр источников данных с URL и TTL | ET4 |

**Новое в v1.2.0:** в C1 добавлены feature-флаги `opendata_csv_enabled`, `power_outages_url`, `has_opendata_csv` — позволяют гибко включать/отключать источники для разных городов без изменения кода.

**Новое в v1.3.0 — Volume persistence:** файлы C3 (регламенты) и словарь (`vocabulary.yaml`) при рантайм-редактировании сохраняются в `data/` (Railway Volume), а не в `config/` (Docker image). Стратегия dual-path:

1. **Seed** (`config/`) — эталонные файлы в Docker image, обновляются через git
2. **Runtime** (`data/rules/`, `data/vocabulary.yaml`) — Volume, переживает деплои
3. **Startup hook** `_seed_volume_data()` — копирует seed → Volume если файл отсутствует (never overwrite)
4. Код читает из `data/` первым, фолбэк на `config/`

***

## Слой 3 — Gateway: api.py (FastAPI)

`api.py` — **точка входа**, больше не содержит бизнес-логики. Выполняет: создание app, CORS middleware, startup events, регистрацию роутеров, кастомный Swagger UI, SSE `/run-tests`.

### Зарегистрированные роутеры

| Модуль | Prefix | Тег | Содержание |
| :-- | :-- | :-- | :-- |
| `routes/data.py` | `/` | Запросы / Данные / Управление | Главный Q\&A (`/ask`), `opendata`, `/update` |
| `routes/ecology.py` | `/ecology` | Экология | AQI, PM2.5, PM10, погода, история |
| `routes/transport.py` | `/transport` | Транспорт | Маршруты 2GIS, остановки |
| `routes/cameras.py` | `/cameras` | Камеры | Камеры ПДД (OSM) |
| `routes/medical.py` | `/medical` | Медицина | Медучреждения (OSM) |
| `routes/twogis.py` | `/2gis` | 2GIS | Управление API-ключом 2GIS |
| `routes/ciinsu.py` | `/ciinsu` | ЦИИ НГУ | TF-поиск по базе ЦИИ НГУ |
| `routes/studio.py` | `/studio` | Studio | Data Studio SPA |
| `routes/admin.py` | `/admin` | Управление | Регламенты, пароль, сервисные операции |

### Системные эндпоинты в api.py

| Маршрут | Метод | Назначение |
| :-- | :-- | :-- |
| `/` | GET | Веб-интерфейс (`index.html`) |
| `/docs` | GET | Swagger UI с кастомной навигацией |
| `/run-tests` | GET (SSE) | Стриминг pytest + health-check данных |
| `/static/*` | GET | Статические файлы (иконки, фавиконы) |

### Startup Events

Пять событий при запуске сервера:

1. `_load_saved_api_keys` — загружает `.env`, затем `data/api_keys.json`
2. `_seed_volume_data` — копирует seed-файлы из `config/` → `data/` (регламенты, словарь) если отсутствуют
3. `_seed_ecology_history` — заглушки ecology за последние 20 дней
4. `_geocode_metro_stations` — фоновое геокодирование метро через 2GIS (задержка 5 с)
5. `_start_background_preloader` — запуск `preload_all_async` (15 с) + `periodic_refresh_loop`

***

## Слой 4 — Семантическое ядро (CORE)

**Глоссарий.** Confidence-score — числовая оценка уверенности классификатора; при низком значении система запрашивает уточнение или выбирает наиболее вероятный интент по умолчанию.


| ID | Файл | Роль | Вход | Выход |
| :-- | :-- | :-- | :-- | :-- |
| R | `router.py` | Классификатор интентов: 14 тем, confidence-score, стемминг, NER топонимов | текст + топонимы из RF1 | `RouteResult` |
| P | `planner.py` | Построитель плана исполнения | `RouteResult` | `ExecutionPlan` |
| RE | `rule_engine.py` | Движок регламентных правил: пороги критичности, эскалация, сезонность | `ExecutionPlan` + C3 | обогащённый план |
| TI | `traffic_index.py` | Детерминированный расчёт транспортного индекса | данные + погода + календарь | числовой индекс [0–10] |

**Новое в v1.2.0:** router поддерживает 14 тем (добавлены `cameras`, `medical`, `construction`) .

### Поддерживаемые темы и операции роутера

| Тема | Объектов | Тип операции |
| :-- | :-- | :-- |
| `parking` | ~2 360 | COUNT, GROUP, FILTER, TOP_N |
| `stops` | ~746 | COUNT, FILTER |
| `schools` | ~214 | COUNT, GROUP, FILTER |
| `kindergartens` | ~253 | COUNT, GROUP, FILTER |
| `libraries` | ~11 | FILTER |
| `pharmacies` | ~27 | FILTER, TOP_N |
| `sport_grounds` | ~142 | COUNT, FILTER |
| `sport_orgs` | ~89 | FILTER |
| `culture` | ~11 | FILTER |
| `cameras` | ~60 (OSM) | FILTER |
| `medical` | ~100+ (OSM) | COUNT, FILTER |
| `power_outages` | real-time | POWER_STATUS, POWER_TODAY, POWER_PLANNED, POWER_HISTORY |
| `ecology` | real-time | ECO_STATUS, ECO_PDK, ECO_HISTORY, AQI_EXCEEDANCE |
| `construction` | ~7 877 | CONSTRUCTION_ACTIVE, CONSTRUCTION_COMMISSIONED |


***

## Слой 5 — Справочники города (REF)

| ID | Файл | Данные |
| :-- | :-- | :-- |
| RF1 | `city_config.py` | City Abstraction Object: районы, топонимы, координаты — загружается из C1 |
| RF2 | `metro_data.py` | Станции, линии, переходы метро НСК |
| RF3 | `heat_sources.py` | Котельные и ТЭЦ с координатами |
| RF4 | `airport_data.py` | Аэропорт Толмачёво — терминалы, транспорт |
| RF5 | `emissions.py` | Выбросы загрязняющих веществ (форма 2-ТП Воздух) |
| RF6 | `constants.py` | Типы ресурсов ЖКХ, шкалы критичности, глобальные словари |


***

## Слой 6 — ETL: Адаптеры и Кеши

| ID | Файл | Тип | Назначение |
| :-- | :-- | :-- | :-- |
| ET1 | `fetcher.py` | Base | Универсальный HTTP-клиент: retry, timeout, headers |
| ET2 | `*_fetcher.py` / `*_scraper.py` | Adapters | Предметные адаптеры: экология, ЖКХ (НСК+Омск), камеры, медицина, стройки, транспорт |
| ET3 | `*_cache.py` | Cache | DuckDB-кеши по доменам: ecology, power, construction, cameras, medical |
| ET4 | `updater.py` | Scheduler | TTL-шедулер фонового обновления; читает C4; запускает `preload_all_async` + `periodic_refresh_loop` |
| ET5 | `parser.py` | Transform | CSV-нормализатор: кодировка, разделитель, canonical schema |
| ET6 | `registry.py` | Registry | Реестр тем: загружает `datasets.yaml`, отдаёт метаданные для health-check |


***

## Слой 7 — Хранилище (STORE)

| ID | Тип | Содержимое | Персистентность |
| :-- | :-- | :-- | :-- |
| DB | DuckDB in-process | Аналитические запросы по всем доменным таблицам | Volume (`data/cities/*/cache.db`) |
| FS | `data/*.json / *.csv` | Метаданные кешей, api_keys.json, сырые данные | Volume (`data/`) |
| VOL | Railway Volume | Монтируется в `/app/data` — переживает деплои | Persistent |
| IMG | Docker image (`config/`) | Seed-файлы: профили, схемы, эталонные регламенты | Пересоздаётся при деплое |

**Стратегия персистентности:** весь каталог `data/` монтируется как Railway Volume. Docker image содержит `config/` с эталонными файлами. При startup хук `_seed_volume_data()` копирует отсутствующие файлы из image → Volume (never overwrite). Рантайм-изменения (регламенты, словарь, API-ключи) сохраняются в Volume и не теряются при деплое.

```
Docker image (пересоздаётся)     Railway Volume (персистентный)
config/                          data/
├── city_profile.yaml            ├── cities/novosibirsk/cache.db    ← DuckDB
├── canonical_schemas.yaml       ├── rules/*.yaml                   ← рантайм-регламенты
├── datasets.yaml                ├── vocabulary.yaml                ← рантайм-словарь
└── rules/*.yaml  ──seed──→      ├── api_keys.json                 ← ключи
                                 ├── ciinsu/news.json               ← контент
                                 └── raw/                           ← CSV-кэш
```


***

## Слой 8 — Рендеринг и исполнение (RENDER)

| ID | Файл | Роль |
| :-- | :-- | :-- |
| EX | `executor.py` | Оркестратор вызовов: исполняет `ExecutionPlan`, вызывает кеши, гео, рендер |
| RN | `renderer.py` | HTML-карточки, JSON для карты, ролевые шаблоны |
| CI | `ciinsu.py` | TF-поиск по базе ЦИИ НГУ |
| GEO | `geocoder.py` | Геокодирование адресов через 2GIS API |


***

## Слой 9 — Фронтенд (static/)

| ID | Файл | Содержимое |
| :-- | :-- | :-- |
| F1 | `index.html` | Карта 2GIS MapGL + чат-интерфейс + экоотчёт за 30 дней |
| F2 | `studio.html` | Data Studio SPA — обзор, регламенты, импорт, мониторинг |
| F3 | `news-editor.html` | Редактор городских новостей ЦИИ НГУ |
| F4 | `mobile.html` | Мобильный пульт презентации |
| F5 | `display.html` | Экран презентации (большой монитор) |
| F6 | `info.html` | Страница информации |


***

## Диаграмма потока (Mermaid)

```mermaid
flowchart TB
  subgraph USERS["👥 Пользователи"]
    U1["🧑 Горожанин\nWeb / голос"]
    U2["🏛 Чиновник\nrole=official"]
    U3["📊 Аналитик\nData Studio"]
  end

  subgraph EXT["🌐 Внешние источники"]
    E1["opendata NSK\nCSV"]
    E2["Open-Meteo\nJSON (бесплатно)"]
    E3["2GIS MapGL + Routes"]
    E4["051.nsk.ru\nHTML scraping"]
    E6["OSM Overpass API\nкамеры + медицина"]
  end

  subgraph CAL["⚙️ City Abstraction Layer"]
    subgraph IMG["Docker image — config/"]
      C1["city_profile_*.yaml\nfeature-флаги"]
      C2["canonical_schemas.yaml"]
      C3_seed["rules/*.yaml\n(seed)"]
      C4["datasets.yaml"]
    end
    subgraph VOL["Railway Volume — data/"]
      C3_rt["data/rules/*.yaml\n(runtime)"]
      VOCAB["data/vocabulary.yaml"]
      KEYS["data/api_keys.json"]
      DB["DuckDB\ncities/*/cache.db"]
    end
    C3_seed -.->|seed on first deploy| C3_rt
  end

  subgraph GW["🚪 Gateway — api.py"]
    G0["startup events\nseed + preload"]
    G1["routes/data.py\n/ask /update"]
    G2["routes/studio.py\n/studio"]
    G3["routes/admin.py\n/admin/rules + storage"]
    G4["routes/ecology.py\n/ecology/* + aqi-exceedances"]
    G5["routes/cameras.py\nroutes/medical.py"]
    G6["/run-tests SSE"]
  end

  subgraph CORE["🧠 Семантическое ядро"]
    R["router.py\n14 интентов + confidence"]
    P["planner.py\nExecutionPlan"]
    RE["rule_engine.py\n5 регламентов"]
    TI["traffic_index.py"]
  end

  subgraph ETL["🔄 ETL"]
    ET1["fetcher.py\nBase HTTP"]
    ET2["*_fetcher / *_scraper"]
    ET3["*_cache.py DuckDB"]
    ET4["updater.py TTL-шедулер"]
    ET6["registry.py"]
  end

  subgraph RENDER["🎨 Рендеринг"]
    EX["executor.py"]
    RN["renderer.py"]
    GEO["geocoder.py"]
  end

  U1 -->|POST /ask| G1
  U3 --> G2
  G0 -.->|_seed_volume_data| VOL
  G1 --> R
  R -->|RouteResult| P
  P -->|ExecutionPlan| RE
  RE -->|enriched plan| EX
  TI --> EX
  C1 --> R
  C3_rt --> RE
  C4 --> ET4
  EX --> ET3
  EX --> GEO
  EX --> RN
  ET4 --> ET1
  ET2 --> ET1
  ET1 --> E1
  ET1 --> E2
  ET1 --> E4
  ET1 --> E6
  ET2 --> ET3
  ET3 --> DB
  RN --> G1
  G1 -->|JSON| U1
```


***

## Текущий технический долг (v1.3.0)

| \# | Проблема | Приоритет | Предлагаемое решение |
| :-- | :-- | :-- | :-- |
| TD-01 | `routes/data.py` — самый крупный роутер | Средний | Вынести обработчики `construction`, `cameras` в отдельные модули |
| TD-02 | `routes/ecology.py` — содержит логику агрегации | Средний | Выделить `ecology_service.py` с чистой бизнес-логикой |
| TD-03 | `routes/studio.py` — смешаны SPA и API | Низкий | Разделить `studio_pages.py` (HTML) и `studio_api.py` (JSON endpoints) |
| TD-04 | Нет Auth-слоя (CORS `allow_origins=["*"]`) | Высокий | Добавить `routes/auth.py`: JWT или API-key middleware |
| TD-05 | `_load_dotenv()` — ручная реализация | Низкий | Заменить на `python-dotenv` |
| ~~TD-06~~ | ~~Конфигурация теряется при деплое~~ | ~~Решено~~ | Volume persistence + dual-path (v1.3.0) |


***

## Реестр файлов src/ (актуальный)

```
src/
├── api.py                  # Точка входа FastAPI (GW) + _seed_volume_data()
├── routes/                 # Роутеры (GW слой)
│   ├── data.py             # /ask, /update, opendata
│   ├── ecology.py          # /ecology/*, /ecology/aqi-exceedances
│   ├── studio.py           # /studio
│   ├── transport.py        # /transport/*
│   ├── cameras.py          # /cameras/*
│   ├── medical.py          # /medical/*
│   ├── twogis.py           # /2gis/*
│   ├── ciinsu.py           # /ciinsu/*
│   └── admin.py            # /admin/*, /admin/storage-stats
├── router.py               # CORE: классификатор 14 интентов
├── planner.py              # CORE: ExecutionPlan
├── rule_engine.py          # CORE: 5 регламентов (dual-path data/ → config/)
├── traffic_index.py        # CORE: индекс пробок
├── vocabulary.py           # CORE: словарь терминов (dual-path data/ → config/)
├── executor.py             # RENDER: оркестратор
├── renderer.py             # RENDER: форматирование
├── geocoder.py             # RENDER: геокодирование
├── city_config.py          # REF: City Abstraction Object
├── metro_data.py           # REF: метро НСК
├── heat_sources.py         # REF: ТЭЦ
├── airport_data.py         # REF: аэропорт
├── emissions.py            # REF: выбросы 2-ТП
├── constants.py            # REF: словари
├── fetcher.py              # ETL: Base HTTP
├── updater.py              # ETL: TTL-шедулер
├── registry.py             # ETL: реестр тем
├── parser.py               # ETL: CSV-нормализатор
├── *_fetcher.py            # ETL: предметные адаптеры
├── *_cache.py              # ETL: DuckDB-кеши (ecology: fact_measurements + ecology_daily_archive)
├── ciinsu.py               # RENDER: TF-поиск ЦИИ НГУ
└── static/                 # FRONT
    ├── index.html           # Чат + карта + экоотчёт 30 дней
    ├── studio.html          # Data Studio SPA
    ├── mobile.html          # Мобильный пульт презентации
    ├── display.html         # Экран презентации
    ├── info.html            # Информационная страница
    └── news-editor.html     # Редактор новостей ЦИИ НГУ
```


