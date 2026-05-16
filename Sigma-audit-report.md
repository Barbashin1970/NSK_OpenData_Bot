# Sigma-Python audit report

**Project:** NSK OpenData Bot · v1.5.0
**Files scanned:** 62 (`src/**/*.py`, исключая `__pycache__`, `tests/`, `test_*.py`)
**Lines of Python:** 20 579
**Sigma-Python score:** **94.3 / 100**

> ESLint baseline pass: not applicable — этот аудит запущен только для backend (Python).
> sigma-audit CLI: не установлен в окружении, аудит выполнен через AST + grep с верификацией чтением.

---

## Summary

| Rule | Severity | Violations | Compliant occurrences |
|---|---|---:|---:|
| P1 — bounded iteration                | Error    | 4   | 167 |
| P2 — controlled recursion             | Error    | 0   | — (нет рекурсии) |
| P3 — no global in recursion           | Error    | 0   | — |
| P4 — no exp. recursion / def in loops | Error    | 0   | — |
| P5 — built-ins / comprehensions       | Warning  | 10  | 410 |
| P6 — stable iteration                 | Error    | 0   | — |
| P7 — linear string building           | Warning  | 0   | 72 (`.join(...)`) |
| P8.1 — await-in-loop                  | Error    | 11  | 8 (`asyncio.create_task`) |
| P8.3 — async без await                | Error    | 3   | — |
| P8.4 — async resources via `async with` | Error  | N/A | проект не использует SQLAlchemy/httpx async |
| P9 — DB/ORM boundaries                | Error    | N/A | проект использует DuckDB raw SQL, ORM нет |
| J1 — `for x in range(len(...))`       | Info     | 0   | — |
| J2 — manual accumulator               | Info     | (= P5) | — |
| J3 — manual filter+map                | Info     | (= P5) | — |
| J4 — `if k not in d: d[k]=...`        | Info / Warning (cluster) | 9 (вкл. 1 кластер) | 7 (`defaultdict`+`setdefault`) |
| J5 — `len(x) > 0`                     | Info     | 0   | — |

**Score breakdown:** errors `18 × 5 = 90` + warnings `11 × 2 = 22` + infos `6 × 1 = 6` = **penalty 118** на **20.58 kLOC** → `100 − 118 / 20.58` = **94.3**.

---

## Top files by violation weight

1. `src/updater.py` — 5 P8.1 + 3 P1 `while True` (фоновые daemon-циклы) — суммарный вес **40**.
2. `src/routes/presenter.py` — 5 P8.1 + 1 P1 `while True` SSE-стрим — суммарный вес **30**.
3. `src/api.py` — 2 P8.3 (`async def` без `await`) — суммарный вес **10**.
4. `src/power_cache.py` — J4-кластер (3×J4) — вес **2** (warning).
5. `src/routes/admin.py` — 1 P8.3 — вес **5**.

---

## Per-rule findings

### P1 — Bounded iteration (4 violations)

Все 4 случая — **бесконечные циклы фоновых daemon-задач** (preloader / refresh / SSE). Они корректно завершаются при остановке процесса, но формально нарушают P1 (нет `MAX_ITER` или сигнала остановки).

`src/updater.py:166`
```python
while True:
    await asyncio.sleep(interval_sec)
    log.info("periodic_refresh_loop: проверка устаревших тем…")
    refreshed = 0
    for topic in PRELOAD_ORDER: ...
```
Рекомендация: `# sigma:allow P1 — фоновый daemon, выходит при cancel()` либо ввести `_shutdown_event` и `while not _shutdown_event.is_set():`.

`src/updater.py:206` — `critical_data_refresh_loop`, та же конструкция.
`src/updater.py:771` — `multi_city_refresh_loop`, та же конструкция.
`src/routes/presenter.py:124` — SSE-стрим (legitimate, отменяется при disconnect).

### P2 — Controlled recursion (0 violations)

В кодовой базе **нет рекурсивных функций** (проверено через `ast.NodeVisitor`: ни прямой `f(...)`, ни косвенной `self.f(...)`). Категория FP-Certified по структурной рекурсии не применима — обходов через explicit stack тоже нет, все обходы — линейные `for` по уже плоским коллекциям.

### P3 — No global in recursion (0 violations)

В коде есть 9 функций с `global ...` (lazy-init кеши: `_kb`, `_registry`, `_terms`, `_HOLIDAY_DB`, `import_in_progress` и др.), но **все они нерекурсивны** → P3 не нарушается.

### P4 — No exponential recursion, no `def` in loops (0 violations)

AST-проверка: ни одной `FunctionDef`/`ClassDef` внутри `for`/`while`/`AsyncFor`. Closures не пересобираются в горячих путях.

### P5 — Built-ins / comprehensions (10 violations)

Конструктивная критика (строгий детектор: только однострочные тела цикла).

`src/ecology_fetcher.py:187`
```python
records = []
for i, day in enumerate(times):
    records.append({...})
```
→ `records = [{...} for i, day in enumerate(times)]`

`src/ecology_fetcher.py:215`, `src/power_cache.py:547`, `src/contractors_loader.py:115`,
`src/city_config.py:166`, `src/api.py:277`, `src/routes/admin.py:194`,
`src/routes/data.py:1341` — аналогичные `manual_map_to_append`, кандидаты на list-comprehension.

`src/updater.py:399`
```python
for poly in geom.get("coordinates", []):
    if poly:
        result.append({"district": district, "polygon": poly[0]})
```
→ `result.extend({"district": district, "polygon": p[0]} for p in geom.get("coordinates", []) if p)`

`src/district_classifier.py:78` — аналогичный filter+map → comprehension.

> **Замечание о ложных срабатываниях.** Шире-первый детектор находил 104 «P5» — большинство были многоэтапные агрегации (`for ... if ... else ...` с side-effects, например в `power_cache.py:610` накопление гистограмм по часам), которые **не сводятся** к comprehension без потери читаемости. Они не учтены в финальном счёте.

### P6 — Stable iteration (0 violations)

Единственный случай мутации словаря (`del _sessions[sid]` в `presenter.py:44`) выполняется по **отдельной коллекции `expired`**, собранной до цикла. Это корректный P6-паттерн.

### P7 — Linear string building (0 violations)

72 использования `.join(...)`. Ни одного `s += ...` со строкой в цикле.

### P8.1 — Sequential await in loop (11 violations)

Большая часть — daemon-loops, где sequential await обусловлен **rate-limiting** (`asyncio.sleep(_PRELOAD_INTERVAL)` между итерациями) или **stream-источником** (SSE). По CLAUDE.md это валидный кейс и требует пометки `# sigma:allow P8 — sequential by design (rate-limit / stream)`.

`src/updater.py:128` — `preload_all_async`: грузит темы по очереди с задержкой 1 сек.
`src/updater.py:166`, `170`, `206`, `771`, `776` — daemon-loops.
`src/routes/presenter.py:124`, `165`, `200`, `222`, `240` — SSE-стримы и waiters.

Реальный P8.1 риск отсутствует — нигде нет ситуации «N независимых HTTP-запросов в цикле». Параллельный fan-out не нужен.

### P8.3 — `async def` без `await` (3 violations)

`src/api.py:354 _seed_task_space` — `async def`, но тело синхронное (вызовы `seed_contractors`, `seed_users`, `seed_initiatives` — все sync).
`src/api.py:378 _start_background_preloader` — async def, но всё содержимое — `asyncio.create_task(...)`. Это **легитимно**: `create_task` требует event loop, который доступен в `async def` startup hook.
`src/routes/admin.py:237 dev_password_change` — async POST handler, но никаких I/O нет.

Рекомендация: `_seed_task_space` и `dev_password_change` — заменить на `def`. `_start_background_preloader` — оставить, добавить `# sigma:allow P8.3 — нужен event loop для create_task`.

### P8.4 / P9 — N/A

Проект не использует SQLAlchemy / httpx / aiofiles. Все БД-операции — синхронный DuckDB через `duckdb.connect(...).execute(...)`. ORM-relationship-loading и `selectinload`/`joinedload` не применимы.

### J4 — `if k not in d: d[k] = ...` (9 occurrences, 1 cluster)

**Architectural J-cluster: 3 × J4 в `src/power_cache.py`** (lines 510, 536, 602) — все три внутри агрегационных циклов, кандидаты на `defaultdict(dict)`. Cluster severity = Warning (penalty 2), индивидуальные penalty обнулены.

Остальные **6 индивидуальных** случаев (Info severity, penalty 1 каждый):
- `src/power_scraper_omsk.py:168`
- `src/contractors_loader.py:105`
- `src/registry.py:22`
- `src/routes/transport.py:121`
- `src/routes/data.py:681`
- `src/routes/ecology.py:357`

Пример (`src/power_cache.py:600-606`):
```python
dist_data: dict[str, dict[str, dict]] = {}
for district, day, hour, dow, houses in rows:
    if district not in dist_data:
        dist_data[district] = {}
    if day not in dist_data[district]:
        dist_data[district][day] = {"hours": {}, "dow": int(dow)}
    dist_data[district][day]["hours"][int(hour)] = int(houses)
```
→
```python
from collections import defaultdict
dist_data: dict[str, dict[str, dict]] = defaultdict(dict)
for district, day, hour, dow, houses in rows:
    day_entry = dist_data[district].setdefault(day, {"hours": {}, "dow": int(dow)})
    day_entry["hours"][int(hour)] = int(houses)
```

### J5 — `len(x) > 0` в условиях (0 violations)

4 случая `len(x) > 0` найдены — все в **присваиваниях** (`has_evening = len(evening) > 0`), не в `if`. Это допустимый стиль: явный возврат `bool`. J5 целит именно в `if len(x) > 0:` truthiness anti-pattern, которого в коде нет.

---

## Acknowledged violations (`sigma:allow`)

В коде нет ни одного маркера `# sigma:allow ...`. Если P1 daemon-loops и P8.1 sequential-by-design будут размечены — **счёт вырастет на ~10 пунктов** (errors P1×4 + P8.1×8 daemon-кейсов = 60 пунктов снимутся).

После пометок ожидаемый score: **~99 / 100**.

---

## Recommendations

**Приоритет 1 — Error severity (90 / 118 = 76% штрафа):**

1. Пометить 4 daemon-loops и 8 sequential-by-design await-в-цикле кейсов как `# sigma:allow Pn — <reason>`. Это не меняет код, но снимает ~60 пунктов штрафа.
2. `src/api.py:354` `_seed_task_space` и `src/routes/admin.py:237` `dev_password_change` → конвертировать в обычные `def` (FastAPI поддерживает оба варианта). Снимет 10 пунктов.
3. (Опционально) Ввести `_shutdown_event = asyncio.Event()` в `updater.py` и заменить `while True:` на `while not _shutdown_event.is_set():` — даёт чистое graceful-shutdown поведение.

**Приоритет 2 — Warning severity (22 / 118 = 19%):**

4. Переписать 10 P5-кандидатов на comprehensions (см. список выше). Минимальный риск, читаемость растёт.
5. Рефакторить J4-кластер в `src/power_cache.py` на `defaultdict` (510, 536, 602) — сделать одним коммитом.

**Приоритет 3 — Info severity (6 / 118 = 5%):**

6. Остальные 6 J4 — `setdefault(...)` или `defaultdict(...)` по контексту.

**Что НЕ требуется:**

- P2/P3/P4/P6/P7 чистые — рекурсии нет, мутаций при итерации нет, `+= str` в циклах нет.
- ORM правил (P9) и `async with` (P8.4) проект не задевает архитектурно.

---

## Scoring breakdown

| Severity | Count | Weight | Subtotal |
|---|---:|---:|---:|
| Error    | 18 | × 5 |  90 |
| Warning  | 11 | × 2 |  22 |
| Info     |  6 | × 1 |   6 |
| **Total penalty** |    |     | **118** |
| **kLOC** |    |     | 20.58 |
| **Score** | | | **100 − 118/20.58 = 94.3 / 100** |

---

## Audit metadata

- **Audit mode:** read-only, без модификации файлов.
- **Detector:** AST через `ast.NodeVisitor` (Python 3.12) + grep с верификацией чтением 5–10 окружающих строк.
- **Limitations:** P5-детектор работает строго (только однострочные тела цикла); многоэтапные агрегации не считаются нарушениями — это снижает шум, но может пропустить часть кандидатов на comprehension.
- **Excluded:** `__pycache__/`, `tests/`, `test_*.py`, `.venv/`, `build/`, `dist/`.
- **Frontend pass:** не запускался (запрошен только Python audit).
