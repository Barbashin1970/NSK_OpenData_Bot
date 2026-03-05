"""FastAPI HTTP API для NSK OpenData Bot (bot serve)."""

import json
import re as _re
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    from fastapi import FastAPI, Query
    from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
    from fastapi.middleware.cors import CORSMiddleware
except ImportError:
    raise ImportError("Установите fastapi: pip install fastapi uvicorn")

from .registry import load_registry
from .router import route, best_topic
from .planner import make_plan, INFO_PATTERNS, DISTRICTS_PATTERNS
from .executor import execute_plan
from .fetcher import load_meta, is_stale
from .cache import get_table_info, table_exists

_STATIC = Path(__file__).parent / "static"

_API_DESCRIPTION = """
Естественно-языковой интерфейс к открытым данным мэрии Новосибирска.

## Быстрый старт

Задайте любой вопрос на русском языке через `/ask`:

```
GET /ask?q=сколько+парковок+по+районам
GET /ask?q=школы+в+советском+районе
GET /ask?q=отключения+электричества+сейчас
GET /ask?q=топ-5+аптек+в+центральном+районе
```

## Источники данных

| Источник | TTL | Что содержит |
|---|---|---|
| [opendata.novo-sibirsk.ru](http://opendata.novo-sibirsk.ru) | 24 ч | Парковки, школы, аптеки, библиотеки и др. |
| [051.novo-sibirsk.ru](http://051.novo-sibirsk.ru) | 30 мин | Отключения ЖКХ в реальном времени |

## Поддерживаемые темы

| ID | Название | Объектов |
|---|---|---|
| `parking` | Парковки | ~2 360 |
| `stops` | Остановки транспорта | ~746 |
| `schools` | Школы | ~214 |
| `kindergartens` | Детские сады | ~253 |
| `libraries` | Библиотеки | ~11 |
| `pharmacies` | Аптеки | ~27 |
| `sport_grounds` | Спортплощадки | ~142 |
| `sport_orgs` | Спортивные организации | ~89 |
| `parks` | Парки | ~1 |
| `culture` | Организации культуры | ~11 |
| `power_outages` | Отключения ЖКХ | реальное время |

## Типы операций

Бот автоматически определяет тип по ключевым словам запроса:

| Слова в запросе | `operation` в ответе | Описание |
|---|---|---|
| «сколько», «количество» | `COUNT` | Подсчёт записей |
| «по районам», «по типам» | `GROUP` | Группировка с подсчётом |
| «топ-5», «первые N» | `TOP_N` | Рейтинг по числовому полю |
| «покажи», «список», «все» | `FILTER` | Фильтрация и вывод |
| «сейчас», «текущий» | `POWER_STATUS` | Активные отключения |
| «сегодня» | `POWER_TODAY` | Отключения за сегодня |
| «плановые», «план» | `POWER_PLANNED` | Запланированные отключения |
| «история», «за неделю» | `POWER_HISTORY` | История за N дней |

## Районы Новосибирска

Дзержинский · Железнодорожный · Заельцовский · Калининский · Кировский ·
Ленинский · Октябрьский · Первомайский · Советский · Центральный

---

[← Вернуться в веб-интерфейс](/)
"""

_TAGS_METADATA = [
    {
        "name": "Запросы",
        "description": "Основной интерфейс: задать вопрос на русском языке и получить данные.",
    },
    {
        "name": "Данные",
        "description": "Информация о доступных наборах данных и состоянии кэша.",
    },
    {
        "name": "Управление",
        "description": "Загрузка и обновление данных из внешних источников.",
    },
]

app = FastAPI(
    title="NSK OpenData Bot",
    description=_API_DESCRIPTION,
    version="1.0.0",
    openapi_tags=_TAGS_METADATA,
    contact={"name": "ЦИИ НГУ"},
    license_info={"name": "MIT"},
    docs_url=None,   # кастомный /docs ниже
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Кастомный Swagger UI: навигационная панель с кнопкой «← На главную» ───────
_NAV_BAR_HTML = """
<style>
  /* ── Nav bar ─────────────────────────────────────────────────────────────── */
  #nsk-nav {
    position: fixed; top: 0; left: 0; right: 0; z-index: 9999;
    background: linear-gradient(90deg, #1e3a5f 0%, #1d4ed8 100%);
    color: white; padding: 9px 20px;
    display: flex; align-items: center; gap: 14px;
    box-shadow: 0 2px 10px rgba(0,0,0,.35);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 13px; line-height: 1;
  }
  #nsk-nav a.back {
    color: #93c5fd; text-decoration: none; font-weight: 500;
    display: flex; align-items: center; gap: 5px;
    padding: 5px 13px; border-radius: 8px;
    border: 1px solid rgba(255,255,255,.25);
    transition: background .15s, border-color .15s;
    white-space: nowrap;
  }
  #nsk-nav a.back:hover { background: rgba(255,255,255,.13); border-color: rgba(255,255,255,.45); }
  #nsk-nav .sep { opacity: .3; }
  #nsk-nav .title { font-weight: 700; color: #e2eaf4; font-size: 14px; }
  #nsk-nav .sub { color: #94a3b8; font-size: 12px; }
  #nsk-nav .badge {
    margin-left: auto; background: rgba(255,255,255,.1);
    border: 1px solid rgba(255,255,255,.2); border-radius: 6px;
    padding: 2px 9px; font-size: 11px; color: #cbd5e1;
  }
  #nsk-test-toggle {
    background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.28);
    color: #e2e8f0; border-radius: 8px; padding: 5px 13px;
    font-size: 12px; font-weight: 600; cursor: pointer;
    display: flex; align-items: center; gap: 6px;
    transition: background .15s, border-color .15s;
    white-space: nowrap;
  }
  #nsk-test-toggle:hover { background: rgba(255,255,255,.22); border-color: rgba(255,255,255,.5); }
  #nsk-test-toggle .dot {
    width: 7px; height: 7px; border-radius: 50%;
    background: #94a3b8; display: inline-block;
    transition: background .3s;
  }
  #nsk-test-toggle .dot.running { background: #facc15; animation: nsk-pulse 1s infinite; }
  #nsk-test-toggle .dot.ok      { background: #4ade80; }
  #nsk-test-toggle .dot.fail    { background: #f87171; }
  @keyframes nsk-pulse { 0%,100% { opacity:1; } 50% { opacity:.4; } }
  body { padding-top: 44px !important; }

  /* ── Test panel ──────────────────────────────────────────────────────────── */
  #nsk-panel {
    position: fixed; bottom: 0; right: 24px; width: 540px;
    background: #0f172a; border: 1px solid #1e3a5f; border-bottom: none;
    border-radius: 12px 12px 0 0;
    box-shadow: 0 -6px 32px rgba(0,0,0,.6);
    display: flex; flex-direction: column;
    z-index: 9998;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 13px; color: #e2e8f0;
    transform: translateY(100%);
    transition: transform .28s cubic-bezier(.4,0,.2,1);
    max-height: 520px;
  }
  #nsk-panel.open { transform: translateY(0); }

  #nsk-panel-header {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 16px; border-bottom: 1px solid #1e3a5f;
    background: #0d1f3c; border-radius: 12px 12px 0 0;
    cursor: pointer; user-select: none;
  }
  #nsk-panel-header .ph-title { font-weight: 700; font-size: 13px; color: #93c5fd; flex:1; }
  #nsk-panel-header .ph-close {
    background: none; border: none; color: #64748b;
    font-size: 16px; cursor: pointer; padding: 0 4px; line-height: 1;
    transition: color .15s;
  }
  #nsk-panel-header .ph-close:hover { color: #f1f5f9; }

  #nsk-panel-progress-wrap {
    padding: 10px 16px 4px; background: #0f172a;
  }
  #nsk-panel-progress-bg {
    background: #1e293b; border-radius: 6px; height: 10px; overflow: hidden;
  }
  #nsk-panel-progress-fill {
    height: 100%; width: 0%; border-radius: 6px;
    background: linear-gradient(90deg, #3b82f6, #60a5fa);
    transition: width .25s ease;
  }
  #nsk-panel-pct {
    display: flex; justify-content: space-between; align-items: center;
    margin-top: 5px; font-size: 11px; color: #94a3b8;
  }
  #nsk-panel-pct .pct-num { font-weight: 700; color: #60a5fa; font-size: 13px; }
  #nsk-panel-pct .pct-stat { display: flex; gap: 10px; }
  #nsk-panel-pct .ok-cnt  { color: #4ade80; }
  #nsk-panel-pct .err-cnt { color: #f87171; }

  #nsk-panel-body {
    flex: 1; overflow-y: auto; padding: 8px 0;
    min-height: 140px; max-height: 260px;
  }
  #nsk-panel-body::-webkit-scrollbar { width: 4px; }
  #nsk-panel-body::-webkit-scrollbar-track { background: #0f172a; }
  #nsk-panel-body::-webkit-scrollbar-thumb { background: #334155; border-radius: 2px; }

  .nsk-log-line {
    font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
    font-size: 11.5px; line-height: 1.55;
    padding: 1px 16px; white-space: pre-wrap; word-break: break-all;
    color: #94a3b8;
  }
  .nsk-log-line.passed { color: #4ade80; }
  .nsk-log-line.failed { color: #f87171; }
  .nsk-log-line.warn   { color: #facc15; }
  .nsk-log-line.info   { color: #7dd3fc; }
  .nsk-log-line.dim    { color: #475569; }

  #nsk-panel-health {
    border-top: 1px solid #1e293b; padding: 8px 16px;
    max-height: 130px; overflow-y: auto;
  }
  #nsk-panel-health::-webkit-scrollbar { width: 4px; }
  #nsk-panel-health::-webkit-scrollbar-thumb { background: #334155; }
  .nsk-health-title { font-size: 11px; font-weight: 700; color: #64748b;
                      text-transform: uppercase; letter-spacing: .05em; margin-bottom: 5px; }
  .nsk-health-grid { display: flex; flex-wrap: wrap; gap: 4px; }
  .nsk-hc {
    font-size: 11px; border-radius: 5px; padding: 2px 8px;
    border: 1px solid transparent;
  }
  .nsk-hc.ok      { background:#052e16; border-color:#166534; color:#4ade80; }
  .nsk-hc.stale   { background:#422006; border-color:#92400e; color:#fbbf24; }
  .nsk-hc.missing { background:#3b0764; border-color:#7e22ce; color:#c4b5fd; }

  #nsk-panel-footer {
    border-top: 1px solid #1e3a5f; padding: 10px 16px;
    display: flex; gap: 8px; align-items: center; background: #0d1f3c;
  }
  #nsk-run-btn {
    background: linear-gradient(90deg,#1d4ed8,#2563eb);
    border: none; color: white; border-radius: 8px;
    padding: 7px 18px; font-size: 13px; font-weight: 700;
    cursor: pointer; transition: opacity .15s;
    display: flex; align-items: center; gap: 7px;
  }
  #nsk-run-btn:hover:not(:disabled) { opacity: .85; }
  #nsk-run-btn:disabled { opacity: .45; cursor: not-allowed; }
  #nsk-panel-result {
    flex: 1; font-size: 13px; font-weight: 600;
    display: flex; align-items: center; gap: 6px;
  }
  #nsk-panel-result.ok   { color: #4ade80; }
  #nsk-panel-result.fail { color: #f87171; }
</style>

<div id="nsk-nav">
  <a href="/" class="back">← На главную</a>
  <span class="sep">|</span>
  <span class="title">NSK OpenData Bot</span>
  <span class="sub">API Документация</span>
  <button id="nsk-test-toggle" onclick="NSKTests.toggle()">
    <span class="dot" id="nsk-dot"></span> Тестирование
  </button>
  <span class="badge">v1.0.0</span>
</div>

<div id="nsk-panel">
  <div id="nsk-panel-header" onclick="NSKTests.toggle()">
    <span class="ph-title">Тестирование системы</span>
    <button class="ph-close" onclick="event.stopPropagation();NSKTests.close()">✕</button>
  </div>
  <div id="nsk-panel-progress-wrap">
    <div id="nsk-panel-progress-bg">
      <div id="nsk-panel-progress-fill"></div>
    </div>
    <div id="nsk-panel-pct">
      <span class="pct-num" id="nsk-pct-text">0%</span>
      <span class="pct-stat">
        <span class="ok-cnt"  id="nsk-pass-cnt">— прошли</span>
        <span class="err-cnt" id="nsk-fail-cnt">— упали</span>
      </span>
    </div>
  </div>
  <div id="nsk-panel-body"></div>
  <div id="nsk-panel-health">
    <div class="nsk-health-title">Состояние данных</div>
    <div class="nsk-health-grid" id="nsk-health-grid">
      <span class="nsk-hc stale" style="font-size:11px;border:none;color:#475569">
        Запустите тесты для проверки
      </span>
    </div>
  </div>
  <div id="nsk-panel-footer">
    <button id="nsk-run-btn" onclick="NSKTests.run()">&#9654; Запустить тестирование</button>
    <span id="nsk-panel-result"></span>
  </div>
</div>

<script>
const NSKTests = (() => {
  let source = null;
  let total = 0;
  let passed = 0;
  let failed = 0;
  const panel   = () => document.getElementById('nsk-panel');
  const body    = () => document.getElementById('nsk-panel-body');
  const fill    = () => document.getElementById('nsk-panel-progress-fill');
  const pctTxt  = () => document.getElementById('nsk-pct-text');
  const passCnt = () => document.getElementById('nsk-pass-cnt');
  const failCnt = () => document.getElementById('nsk-fail-cnt');
  const dot     = () => document.getElementById('nsk-dot');
  const runBtn  = () => document.getElementById('nsk-run-btn');
  const result  = () => document.getElementById('nsk-panel-result');
  const hGrid   = () => document.getElementById('nsk-health-grid');

  function toggle() {
    panel().classList.toggle('open');
  }
  function close() {
    panel().classList.remove('open');
    if (source) { source.close(); source = null; }
  }
  function setProgress(pct, p, f, tot) {
    fill().style.width = pct + '%';
    pctTxt().textContent = pct + '%' + (tot ? ' (' + (p+f) + '/' + tot + ')' : '');
    passCnt().textContent = p + ' прошли';
    failCnt().textContent = f + ' упали';
  }
  function addLine(text, cls) {
    const el = document.createElement('div');
    el.className = 'nsk-log-line' + (cls ? ' ' + cls : '');
    el.textContent = text;
    body().appendChild(el);
    body().scrollTop = body().scrollHeight;
  }
  function run() {
    if (source) { source.close(); source = null; }
    panel().classList.add('open');
    body().innerHTML = '';
    result().textContent = '';
    result().className = '';
    setProgress(0, 0, 0, 0);
    runBtn().disabled = true;
    dot().className = 'dot running';
    hGrid().innerHTML = '';
    total = 0; passed = 0; failed = 0;

    source = new EventSource('/run-tests');
    source.onmessage = (e) => {
      const d = JSON.parse(e.data);
      if (d.type === 'health') {
        hGrid().innerHTML = '';
        d.checks.forEach(c => {
          const chip = document.createElement('span');
          chip.className = 'nsk-hc ' + c.status;
          chip.title = c.msg;
          chip.textContent = c.topic + (c.status !== 'ok' ? ' ⚠' : '');
          hGrid().appendChild(chip);
        });
      } else if (d.type === 'start') {
        total = d.total;
        addLine('Собрано тестов: ' + total, 'info');
      } else if (d.type === 'progress') {
        if (d.status === 'passed') {
          passed++;
          addLine('  \u2713 ' + d.short, 'passed');
        } else {
          failed++;
          addLine('  \u2717 ' + d.short, 'failed');
        }
        setProgress(d.pct, passed, failed, d.total);
      } else if (d.type === 'log') {
        if (!d.line.trim()) return;
        const cls = d.line.includes('ERROR') ? 'failed'
                  : d.line.includes('WARNING') ? 'warn'
                  : d.line.startsWith('FAILED') ? 'failed'
                  : d.line.match(/^={3,}/) ? 'dim'
                  : '';
        addLine(d.line, cls);
      } else if (d.type === 'done') {
        source.close(); source = null;
        runBtn().disabled = false;
        setProgress(100, d.passed, d.failed, d.total);
        fill().style.background = d.failed > 0
          ? 'linear-gradient(90deg,#dc2626,#ef4444)'
          : 'linear-gradient(90deg,#16a34a,#4ade80)';
        if (d.failed === 0 && d.returncode === 0) {
          result().textContent = '✓ Все тесты прошли (' + d.passed + ')';
          result().className = 'ok';
          dot().className = 'dot ok';
          addLine('', '');
          addLine('Все ' + d.passed + ' тестов прошли успешно.', 'passed');
        } else {
          result().textContent = '✗ Упало: ' + d.failed;
          result().className = 'fail';
          dot().className = 'dot fail';
          addLine('', '');
          addLine('Упавшие тесты:', 'warn');
          d.failed_lines.forEach(l => addLine('  ' + l, 'failed'));
        }
      }
    };
    source.onerror = () => {
      source.close(); source = null;
      runBtn().disabled = false;
      dot().className = 'dot fail';
      addLine('Ошибка подключения к /run-tests', 'failed');
    };
  }
  return { toggle, close, run };
})();
</script>
"""


@app.get("/run-tests", include_in_schema=False)
def run_tests():
    """SSE-стрим: запускает pytest и отдаёт прогресс + проверку здоровья данных."""

    def _sse(obj: dict) -> str:
        return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

    def generate():
        # ── Проверка здоровья данных ─────────────────────────────────────────
        from .registry import load_registry
        from .fetcher import load_meta, is_stale
        from .cache import table_exists
        from .power_cache import get_power_meta

        registry = load_registry()
        meta = load_meta()
        health_checks = []
        for tid, ds in registry.items():
            if not table_exists(tid):
                health_checks.append({"topic": tid, "status": "missing",
                                      "msg": "Данные не загружены"})
            elif is_stale(tid, ds.get("ttl_hours", 24)):
                rows = meta.get(tid, {}).get("rows", "?")
                health_checks.append({"topic": tid, "status": "stale",
                                      "msg": f"Устаревший кэш ({rows} строк)"})
            else:
                rows = meta.get(tid, {}).get("rows", "?")
                health_checks.append({"topic": tid, "status": "ok",
                                      "msg": f"{rows} строк"})

        try:
            pwr = get_power_meta()
            if pwr.get("last_scraped"):
                health_checks.append({"topic": "power_outages", "status": "ok",
                                      "msg": f"обновлено {pwr['last_scraped']}"})
            else:
                health_checks.append({"topic": "power_outages", "status": "missing",
                                      "msg": "Нет данных об отключениях"})
        except Exception:
            health_checks.append({"topic": "power_outages", "status": "missing",
                                  "msg": "Ошибка при проверке"})

        yield _sse({"type": "health", "checks": health_checks})

        # ── Запуск pytest ────────────────────────────────────────────────────
        project_root = Path(__file__).parent.parent
        proc = subprocess.Popen(
            [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short", "--no-header", "-q"],
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        total = 0
        done = 0
        passed = 0
        failed = 0
        failed_lines: list[str] = []

        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if not line:
                continue

            # Количество тестов из строки "collected N items"
            m = _re.search(r"collected (\d+) item", line)
            if m:
                total = int(m.group(1))
                yield _sse({"type": "start", "total": total})
                continue

            # Строки вида "tests/test_router.py::test_foo PASSED"
            if _re.search(r"\s(PASSED|FAILED|ERROR)\s*$", line):
                done += 1
                if "PASSED" in line:
                    passed += 1
                    status = "passed"
                else:
                    failed += 1
                    status = "failed"
                    failed_lines.append(line)
                pct = int(done / total * 100) if total > 0 else 0
                # Короткое имя теста
                short = _re.sub(r"^.*::", "", line).replace(" PASSED", "").replace(" FAILED", "").replace(" ERROR", "")
                yield _sse({"type": "progress", "done": done, "total": total,
                            "pct": pct, "status": status, "short": short, "line": line})
            else:
                yield _sse({"type": "log", "line": line})

        proc.wait()
        yield _sse({
            "type": "done",
            "passed": passed,
            "failed": failed,
            "total": total,
            "returncode": proc.returncode,
            "failed_lines": failed_lines,
        })

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/docs", include_in_schema=False)
def custom_swagger_ui() -> HTMLResponse:
    """Swagger UI с навигационной панелью и кнопкой возврата на главную страницу."""
    from fastapi.openapi.docs import get_swagger_ui_html
    html_resp = get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="NSK OpenData Bot — API",
        swagger_ui_parameters={
            "defaultModelsExpandDepth": -1,   # скрыть схемы по умолчанию
            "docExpansion": "list",            # раскрыть список эндпоинтов
            "tryItOutEnabled": True,           # «Try it out» сразу активен
            "displayRequestDuration": True,    # показывать время ответа
            "filter": True,                    # строка поиска по эндпоинтам
            "persistAuthorization": True,
        },
    )
    html = html_resp.body.decode("utf-8")
    html = html.replace("<body>", f"<body>{_NAV_BAR_HTML}", 1)
    return HTMLResponse(html)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def get_ui() -> str:
    """Веб-интерфейс бота."""
    html_file = _STATIC / "index.html"
    if not html_file.exists():
        return "<h1>Web UI not found</h1><p>Place index.html in src/static/</p>"
    return html_file.read_text(encoding="utf-8")


@app.get(
    "/topics",
    tags=["Данные"],
    summary="Список доступных тем",
    response_description="Массив `topics` с метаданными и состоянием кэша каждой темы",
)
def get_topics() -> dict:
    """
    Возвращает все поддерживаемые темы открытых данных с информацией о состоянии кэша.

    ### Поля каждой темы

    | Поле | Тип | Описание |
    |---|---|---|
    | `id` | string | Идентификатор для `POST /update?topic=<id>` |
    | `name` | string | Русское название |
    | `description` | string | Краткое описание набора данных |
    | `rows` | int or null | Число строк в кэше (`null` — данные не загружены) |
    | `last_updated` | string or null | Дата последней загрузки (ISO 8601) |
    | `stale` | bool | `true` если TTL истёк и рекомендуется обновление |
    | `passport_url` | string | Паспорт набора на opendata.novo-sibirsk.ru |

    Для обновления данных используйте `POST /update?topic=<id>` или `bot update --all` в CLI.
    """
    registry = load_registry()
    meta = load_meta()
    result = []
    for tid, ds in registry.items():
        m = meta.get(tid, {})
        result.append({
            "id": tid,
            "name": ds.get("name"),
            "description": ds.get("description"),
            "rows": m.get("rows"),
            "last_updated": m.get("last_updated"),
            "stale": is_stale(tid, ds.get("ttl_hours", 24)),
            "passport_url": ds.get("passport_url"),
        })
    return {"topics": result}


@app.get(
    "/ask",
    tags=["Запросы"],
    summary="Задать вопрос на русском языке",
    response_description="Результат: operation, rows, count + метаданные темы и кэша",
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "count": {
                            "summary": "COUNT — «сколько школ»",
                            "value": {
                                "query": "сколько школ",
                                "topic": "schools",
                                "topic_name": "Школы",
                                "confidence": 0.857,
                                "operation": "COUNT",
                                "district": None,
                                "street": None,
                                "count": 214,
                                "rows": [],
                                "columns": [],
                                "cache": {"last_updated": "2026-03-04T10:00:00", "rows": 214},
                            },
                        },
                        "group": {
                            "summary": "GROUP — «парковки по районам»",
                            "value": {
                                "query": "парковки по районам",
                                "topic": "parking",
                                "topic_name": "Парковки",
                                "confidence": 0.75,
                                "operation": "GROUP",
                                "district": None,
                                "count": 2360,
                                "rows": [
                                    {"район": "Центральный район", "количество": 456},
                                    {"район": "Советский район", "количество": 312},
                                ],
                                "columns": ["район", "количество"],
                            },
                        },
                        "filter": {
                            "summary": "FILTER — «библиотеки для детей»",
                            "value": {
                                "query": "библиотеки для детей",
                                "topic": "libraries",
                                "operation": "FILTER",
                                "extra_filters": {"audience": "children"},
                                "count": 7,
                                "rows": [{"BiblName": "Детская библиотека № 1", "AdrDistr": "Ленинский район"}],
                                "columns": ["BiblName", "AdrDistr", "AdrStreet", "AdrDom", "Phone", "Site"],
                            },
                        },
                        "power": {
                            "summary": "POWER_STATUS — «отключения электричества сейчас»",
                            "value": {
                                "query": "отключения электричества сейчас",
                                "topic": "power_outages",
                                "topic_name": "Отключения электроснабжения",
                                "operation": "POWER_STATUS",
                                "district": None,
                                "count": 2,
                                "rows": [
                                    {"utility": "Электроснабжение", "group_type": "active",
                                     "district": "Ленинский район", "houses": "14",
                                     "scraped_at": "2026-03-04T09:30:00"},
                                ],
                                "columns": ["utility", "group_type", "district", "houses", "scraped_at"],
                                "power_meta": {"last_scraped": "2026-03-04T09:30:00", "active_houses": 14},
                            },
                        },
                    }
                }
            }
        }
    },
)
def get_ask(
    q: str = Query(
        ...,
        description=(
            "Вопрос на русском языке. Примеры:\n"
            "- `сколько парковок по районам`\n"
            "- `школы в Советском районе`\n"
            "- `топ-5 аптек в центре`\n"
            "- `отключения электричества сейчас`\n"
            "- `плановые отключения света на неделю`\n"
            "- `библиотеки для детей`\n"
            "- `детские спортивные организации по районам`"
        ),
        examples={"default": {"summary": "Группировка по районам", "value": "сколько парковок по районам"}},
        min_length=2,
    ),
) -> dict:
    """
    Основной endpoint. Принимает запрос на русском языке, автоматически определяет
    тему и тип операции, возвращает структурированный результат.

    ### Как работает

    1. **Маршрутизация** — определяет тему по ключевым словам (`router.py`)
    2. **Планирование** — определяет операцию COUNT / GROUP / TOP_N / FILTER / POWER_* (`planner.py`)
    3. **Выполнение** — SQL к DuckDB или скрапинг `051.novo-sibirsk.ru` (`executor.py`)

    ### Поля ответа

    | Поле | Тип | Описание |
    |---|---|---|
    | `query` | string | Исходный запрос |
    | `topic` | string | ID темы (`parking`, `schools`, `power_outages`, …) |
    | `topic_name` | string | Русское название темы |
    | `confidence` | float | Уверенность маршрутизатора (0–1) |
    | `operation` | string | COUNT / GROUP / TOP_N / FILTER / POWER_* |
    | `district` | string or null | Распознанный район (если указан в запросе) |
    | `street` | string or null | Распознанная улица (если указана) |
    | `extra_filters` | object | Доп. фильтры (`audience: children/adults`) |
    | `rows` | array | Строки результата |
    | `columns` | array | Названия колонок |
    | `count` | int | Общее число совпадений (без учёта лимита) |
    | `cache` | object | `last_updated` и `rows` — состояние кэша |

    ### Особые случаи

    - Если тема не определена → поле `error` + список `available_topics`
    - Если данные не загружены → `error` с инструкцией запустить `POST /update`
    - Запросы об отключениях ЖКХ автоматически обновляют кэш если TTL > 30 мин
    """
    route_result = best_topic(q)

    if not route_result:
        q_lower = q.lower()
        if DISTRICTS_PATTERNS.search(q_lower):
            from .router import DISTRICTS
            return {
                "query": q,
                "operation": "DISTRICTS",
                "rows": list(DISTRICTS.keys()),
                "count": len(DISTRICTS),
            }
        if INFO_PATTERNS.search(q_lower):
            registry = load_registry()
            topics_list = [
                {"id": tid, "name": ds.get("name"), "description": ds.get("description")}
                for tid, ds in registry.items()
            ]
            return {"query": q, "operation": "INFO", "topics": topics_list}
        return {
            "query": q,
            "error": "Не удалось определить тему",
            "available_topics": list(load_registry().keys()),
        }

    topic = route_result.topic
    plan = make_plan(q, topic)

    # ── Отключения электроснабжения ──────────────────────────────────────────
    if topic == "power_outages":
        from .executor import execute_power
        from .power_cache import is_power_stale, get_power_meta, upsert_outages
        from .power_scraper import fetch_all_outages

        if is_power_stale():
            upsert_outages(fetch_all_outages())

        result = execute_power(plan)
        meta = get_power_meta()
        return {
            "query": q,
            "topic": topic,
            "topic_name": route_result.name,
            "confidence": round(route_result.confidence, 3),
            "operation": plan.operation,
            "district": plan.district,
            "sub_district": plan.sub_district,
            "power_meta": {k: str(v) if not isinstance(v, (int, float, str, bool, type(None))) else v
                           for k, v in meta.items()},
            **result,
        }

    # ── Стандартные темы opendata ─────────────────────────────────────────────
    if not table_exists(topic):
        return {
            "query": q,
            "topic": topic,
            "error": f"Данные не загружены. POST /update?topic={topic}",
        }

    result = execute_plan(plan)

    cache_info = load_meta().get(topic, {})
    cache_info.update(get_table_info(topic))

    return {
        "query": q,
        "topic": topic,
        "topic_name": route_result.name,
        "confidence": round(route_result.confidence, 3),
        "operation": plan.operation,
        "district": plan.district,
        "sub_district": plan.sub_district,
        "street": plan.street,
        "extra_filters": plan.extra_filters,
        "cache": {
            "last_updated": cache_info.get("last_updated"),
            "rows": cache_info.get("rows"),
        },
        **result,
    }


@app.post(
    "/update",
    tags=["Управление"],
    summary="Обновить данные из источника",
    response_description="Статус обновления по каждой теме: `rows` и `success`",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "updated": {
                            "parking": {"rows": 2360, "success": True},
                            "schools": {"rows": 214, "success": True},
                            "pharmacies": {"rows": 27, "success": True},
                        }
                    }
                }
            }
        }
    },
)
def post_update(
    topic: str | None = Query(
        None,
        description=(
            "ID темы для обновления. Если не указан — обновляются все 10 тем (~1–2 мин).\n\n"
            "Доступные ID: `parking`, `stops`, `schools`, `kindergartens`, "
            "`libraries`, `pharmacies`, `parks`, `sport_grounds`, `sport_orgs`, `culture`"
        ),
        examples={"default": {"summary": "Конкретная тема", "value": "parking"}},
    ),
) -> dict:
    """
    Загружает или обновляет данные с [opendata.novo-sibirsk.ru](http://opendata.novo-sibirsk.ru).

    - **Без `topic`** — обновляет все 10 тем (~1–2 минуты, ~4 МБ)
    - **С `topic`** — обновляет только указанную тему (несколько секунд)

    Данные сохраняются в локальный DuckDB-кэш (`DATA/cache.db`).
    TTL по умолчанию — 24 часа.

    > **Отключения ЖКХ** (`power_outages`) обновляются автоматически при запросах
    > через `/ask` (TTL 30 мин). Принудительно в CLI: `bot power update`.
    """
    from .cli import _do_update
    from .registry import list_topics

    topics_to_update = [topic] if topic else list_topics()
    results = {}
    for t in topics_to_update:
        rows = _do_update(t)
        results[t] = {"rows": rows, "success": rows > 0}

    return {"updated": results}
