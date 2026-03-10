"""FastAPI HTTP API для NSK OpenData Bot (bot serve)."""

import json
import os
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
_API_KEYS_FILE = Path(__file__).parent.parent / "data" / "api_keys.json"


def _load_api_keys() -> dict:
    try:
        if _API_KEYS_FILE.exists():
            return json.loads(_API_KEYS_FILE.read_text("utf-8"))
    except Exception:
        pass
    return {}


def _save_api_keys(keys: dict) -> None:
    _API_KEYS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _API_KEYS_FILE.write_text(json.dumps(keys, ensure_ascii=False, indent=2), "utf-8")


def _get_twogis_key() -> str | None:
    """Возвращает текущий ключ 2GIS: ENV > файл > None."""
    env_key = os.environ.get("TWOGIS_API_KEY", "").strip()
    if env_key:
        return env_key
    return _load_api_keys().get("twogis_key", "").strip() or None

_API_DESCRIPTION = """
Естественно-языковой интерфейс к открытым данным мэрии Новосибирска.

## Источники данных

| Источник | TTL | Что содержит |
|---|---|---|
| [opendata.novo-sibirsk.ru](http://opendata.novo-sibirsk.ru) | 24 ч | Парковки, школы, аптеки, библиотеки и др. |
| [051.novo-sibirsk.ru](http://051.novo-sibirsk.ru) | 30 мин | Отключения ЖКХ: электро, тепло, вода, газ |
| [Open-Meteo](https://open-meteo.com) | 15 мин | PM2.5, PM10, AQI, погода по 11 точкам (бесплатно) |
| CityAir API | 15 мин | Телеметрия физических датчиков (требует `CITYAIR_API_KEY`) |
| [2GIS Public Transport](https://dev.2gis.com/api) | real-time | Маршруты общественного транспорта (требует `TWOGIS_API_KEY`, данные не сохраняются) |
| [OpenStreetMap Overpass API](https://overpass-api.de) | 7 дн | Стационарные камеры фиксации нарушений ПДД · лицензия ODbL |

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
| `culture` | Организации культуры | ~11 |
| `cameras` | Камеры фиксации нарушений ПДД | ~60 (OSM) |
| `power_outages` | Отключения ЖКХ (электро/тепло/вода/газ) | реальное время |
| `ecology` | Качество воздуха + погода | реальное время |
| `construction` | Разрешения на строительство + ввод в эксплуатацию | ~5 942 + ~1 935 |

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
| «история», «за неделю» | `POWER_HISTORY` | История отключений |
| «воздух», «экология», «смог» | `ECO_STATUS` | Качество воздуха + погода |
| «ПДК», «превышен», «опасн» | `ECO_PDK` | Превышения PM2.5 > 35 мкг/м³ |
| «динамика», «тренд», «неделю» | `ECO_HISTORY` | История AQI/PM по дням |
| «камер», «видеофиксац», «радар» | `FILTER` | Список камер с координатами |
| «стройк», «застройщик», «новостройк» | `CONSTRUCTION_ACTIVE` | Активные стройки (разрешения − ввод в эксплуатацию) |
| «ввод в эксплуатацию», «введено» | `CONSTRUCTION_COMMISSIONED` | Объекты, введённые в эксплуатацию |

## Районы Новосибирска и прилегающие территории

Дзержинский · Железнодорожный · Заельцовский · Калининский · Кировский ·
Ленинский · Октябрьский · Первомайский · Советский · Центральный

Также поддерживается: **Кольцово** (наукоград, отдельная точка мониторинга погоды и качества воздуха).
Запросы к открытым данным города (школы, парковки и т.д.) для Кольцово не применимы —
наукоград находится вне 10 административных районов Новосибирска.

"""

_TAGS_METADATA = [
    {
        "name": "Запросы",
        "description": (
            "Основной интерфейс: задать вопрос на русском языке и получить данные.\n\n"
            "Примеры запросов:\n\n"
            "`GET /ask?q=сколько+парковок+по+районам`\n\n"
            "`GET /ask?q=школы+в+советском+районе`\n\n"
            "`GET /ask?q=отключения+электричества+сейчас`\n\n"
            "`GET /ask?q=топ-5+аптек+в+центральном+районе`"
        ),
    },
    {
        "name": "Экология",
        "description": (
            "Качество воздуха (PM2.5, PM10, NO2, AQI) и метеорология по 10 районам Новосибирска. "
            "Источники: Open-Meteo (бесплатно) + CityAir (опционально). TTL = 15 мин."
        ),
    },
    {
        "name": "Данные",
        "description": "Информация о доступных наборах данных и состоянии кэша.",
    },
    {
        "name": "Управление",
        "description": "Загрузка и обновление данных из внешних источников.",
    },
    {
        "name": "Камеры",
        "description": (
            "Стационарные камеры фиксации нарушений ПДД в Новосибирске. "
            "Источник: OpenStreetMap (Overpass API, тег `highway=speed_camera`). "
            "Координаты предзагружены из OSM — геокодирование не требуется. "
            "Лицензия данных: ODbL (openstreetmap.org/copyright). TTL = 7 дней."
        ),
    },
    {
        "name": "2GIS",
        "description": (
            "Управление ключом 2GIS API. Ключ используется для интерактивных карт (MapGL JS), "
            "геокодирования адресов и маршрутов общественного транспорта. "
            "Получить ключ: [platform.2gis.ru](https://platform.2gis.ru)"
        ),
    },
]

app = FastAPI(
    title="NSK OpenData Bot",
    description=_API_DESCRIPTION,
    version="1.0.5",
    openapi_tags=_TAGS_METADATA,
    contact={"name": "ЦИИ НГУ"},
    docs_url=None,   # кастомный /docs ниже
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


_ENV_FILE = Path(__file__).parent.parent / ".env"


def _load_dotenv() -> None:
    """Загружает KEY=VALUE из .env в os.environ (только если переменная ещё не задана)."""
    if not _ENV_FILE.exists():
        return
    try:
        for line in _ENV_FILE.read_text("utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and not os.environ.get(key, "").strip():
                os.environ[key] = val
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("Не удалось загрузить .env: %s", e)


@app.on_event("startup")
def _load_saved_api_keys() -> None:
    """При старте: загружает ключи из .env, затем из data/api_keys.json (если ENV не задан)."""
    # 1. .env файл (создайте его вручную, он в .gitignore и не перезаписывается при обновлениях)
    _load_dotenv()
    # 2. data/api_keys.json (сохраняется через POST /twogis/key)
    if not os.environ.get("TWOGIS_API_KEY", "").strip():
        saved = _load_api_keys().get("twogis_key", "").strip()
        if saved:
            os.environ["TWOGIS_API_KEY"] = saved


@app.on_event("startup")
def _seed_ecology_history() -> None:
    """При старте заполняет ecology_daily_archive заглушками за последние 20 дней.

    Вставка происходит только для дат, которых ещё нет в архиве
    (ON CONFLICT DO NOTHING), поэтому реальные данные не перезаписываются.
    Заглушки (-10 °C, pm25≈12) будут вытеснены реальными показателями
    Open-Meteo при первом же обновлении экологических данных.
    """
    try:
        from .ecology_cache import seed_history_placeholder
        seed_history_placeholder(days=20, temp_c=-10.0)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"seed_ecology_history: {e}")

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
  #nsk-update-btn {
    background: rgba(255,255,255,.08); border: 1px solid rgba(255,255,255,.22);
    color: #cbd5e1; border-radius: 8px;
    padding: 7px 14px; font-size: 12px; font-weight: 600;
    cursor: pointer; transition: background .15s, border-color .15s, color .15s;
    display: flex; align-items: center; gap: 6px; white-space: nowrap;
  }
  #nsk-update-btn:hover:not(:disabled) { background: rgba(255,255,255,.18); color: #f1f5f9; border-color: rgba(255,255,255,.45); }
  #nsk-update-btn:disabled { opacity: .4; cursor: not-allowed; }
  #nsk-update-btn.running { color: #facc15; border-color: #854d0e; background: rgba(250,204,21,.08); }
  #nsk-update-btn.done-ok { color: #4ade80; border-color: #166534; background: rgba(74,222,128,.08); }
  #nsk-update-btn.done-err { color: #f87171; border-color: #991b1b; background: rgba(248,113,113,.08); }
  #nsk-panel-footer { flex-wrap: wrap; row-gap: 6px; }
</style>

<div id="nsk-nav">
  <a href="/" class="back">← На главную</a>
  <span class="sep">|</span>
  <span class="title">NSK OpenData Bot</span>
  <span class="sub">API Документация</span>
  <button id="nsk-test-toggle" onclick="NSKTests.toggle()">
    <span class="dot" id="nsk-dot"></span> Тестирование
  </button>
  <span class="badge">v1.0.5</span>
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
    <button id="nsk-run-btn" onclick="NSKTests.run()">&#9654; Тестирование</button>
    <button id="nsk-update-btn" onclick="NSKTests.updateAll()">&#8635; Загрузить все данные</button>
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
        const _topicLabels = {
          parking:'Парковки', stops:'Остановки', schools:'Школы',
          kindergartens:'Детсады', libraries:'Библиотеки', pharmacies:'Аптеки',
          parks:'Парки', sport_grounds:'Спортплощадки', sport_orgs:'Спортклубы',
          culture:'Культура', power_outages:'Отключения ЖКХ', ecology:'Экология',
        };
        d.checks.forEach(c => {
          const chip = document.createElement('span');
          chip.className = 'nsk-hc ' + c.status;
          chip.title = c.msg;
          chip.textContent = (_topicLabels[c.topic] || c.topic) + (c.status !== 'ok' ? ' ⚠' : '');
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
        if (d.no_pytest) {
          // pytest не установлен в production-окружении
          setProgress(0, 0, 0, 0);
          pctTxt().textContent = 'N/A';
          passCnt().textContent = '— прошли';
          failCnt().textContent = '— упали';
          fill().style.width = '0%';
          fill().style.background = '#334155';
          result().textContent = '⚠ pytest не установлен';
          result().className = '';
          result().style.color = '#94a3b8';
          dot().className = 'dot';
        } else {
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
      }
    };
    source.onerror = () => {
      source.close(); source = null;
      runBtn().disabled = false;
      dot().className = 'dot fail';
      addLine('Ошибка подключения к /run-tests', 'failed');
    };
  }
  async function updateAll() {
    const btn = document.getElementById('nsk-update-btn');
    const runB = runBtn();
    panel().classList.add('open');
    body().innerHTML = '';
    result().textContent = '';
    result().className = '';
    btn.disabled = true;
    runB.disabled = true;
    btn.className = 'running';
    btn.textContent = '⟳ Загружаю данные…';

    addLine('Запуск полного обновления системы…', 'info');
    addLine('Открытые данные · Экология · Отключения ЖКХ', 'dim');
    addLine('', '');

    let totalOk = 0, totalErr = 0;

    // ── 1. Открытые данные (10 наборов) ─────────────────────────────────────
    addLine('▶ Открытые данные мэрии (10 наборов)…', 'info');
    try {
      const resp = await fetch('/update', { method: 'POST' });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const data = await resp.json();
      const updated = data.updated || {};
      for (const [tid, info] of Object.entries(updated)) {
        if (info.success) {
          totalOk++;
          addLine('  ✓ ' + tid + ' — ' + (info.rows || 0) + ' строк', 'passed');
        } else {
          totalErr++;
          addLine('  ✗ ' + tid + ' — ошибка загрузки', 'failed');
        }
      }
    } catch (e) {
      totalErr++;
      addLine('  ✗ Открытые данные: ' + e.message, 'failed');
    }

    // ── 2. Экология и погода ─────────────────────────────────────────────────
    addLine('', '');
    addLine('▶ Экология и погода (Open-Meteo)…', 'info');
    try {
      const resp = await fetch('/ecology/update', { method: 'POST' });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const data = await resp.json();
      if (data.success) {
        totalOk++;
        addLine(
          '  ✓ ecology — ' + (data.records_loaded || 0) + ' измерений · ' +
          (data.districts_covered || 0) + ' районов · ' + (data.source || ''),
          'passed'
        );
      } else {
        totalErr++;
        addLine('  ✗ ecology — данные не загружены', 'failed');
      }
    } catch (e) {
      totalErr++;
      addLine('  ✗ Экология: ' + e.message, 'failed');
    }

    // ── 3. Отключения ЖКХ ───────────────────────────────────────────────────
    addLine('', '');
    addLine('▶ Отключения ЖКХ (051.novo-sibirsk.ru)…', 'info');
    try {
      const resp = await fetch('/power/update', { method: 'POST' });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const data = await resp.json();
      if (data.success) {
        totalOk++;
        addLine(
          '  ✓ power_outages — ' + (data.records_loaded || 0) + ' записей · ' +
          'аварийных: ' + (data.active_houses || 0) + ' д. · ' +
          'плановых: ' + (data.planned_houses || 0) + ' д.',
          'passed'
        );
      } else {
        totalErr++;
        addLine('  ✗ power_outages — данные не загружены', 'failed');
      }
    } catch (e) {
      totalErr++;
      addLine('  ✗ Отключения ЖКХ: ' + e.message, 'failed');
    }

    // ── 4. Камеры фиксации нарушений (OSM) ──────────────────────────────────
    addLine('', '');
    addLine('▶ Камеры фиксации нарушений (OpenStreetMap / Overpass API)…', 'info');
    try {
      const resp = await fetch('/cameras/update', { method: 'POST' });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const data = await resp.json();
      const info = (data.updated || {}).cameras || {};
      if (info.success) {
        totalOk++;
        addLine('  ✓ cameras — ' + (info.rows || 0) + ' камер · OSM ODbL', 'passed');
      } else {
        totalErr++;
        addLine('  ✗ cameras — данные не загружены (Overpass API недоступен?)', 'failed');
      }
    } catch (e) {
      totalErr++;
      addLine('  ✗ Камеры: ' + e.message, 'failed');
    }

    // ── Итог ─────────────────────────────────────────────────────────────────
    addLine('', '');
    if (totalErr === 0) {
      addLine('Все источники данных обновлены (' + totalOk + ' успешно).', 'passed');
      btn.className = 'done-ok';
      btn.textContent = '✓ Все данные загружены';
      result().textContent = '✓ Обновлено (' + totalOk + ')';
      result().className = 'ok';
    } else {
      addLine('Загружено: ' + totalOk + ', ошибок: ' + totalErr + '.', 'warn');
      btn.className = 'done-err';
      btn.textContent = '⚠ Частичная загрузка';
      result().textContent = '⚠ ' + totalErr + ' ошибок';
      result().className = 'fail';
    }
    addLine('Нажмите «Тестирование» чтобы обновить статус данных.', 'dim');
    btn.disabled = false;
    runB.disabled = false;
  }

  return { toggle, close, run, updateAll };
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

        # ── Экология и погода ────────────────────────────────────────────────
        try:
            from .ecology_cache import init_ecology_tables, get_ecology_meta, is_ecology_stale
            init_ecology_tables()
            eco_meta = get_ecology_meta()
            if eco_meta.get("last_updated"):
                stale = is_ecology_stale()
                districts = eco_meta.get("districts_covered", 0)
                ts = str(eco_meta["last_updated"])[:16].replace("T", " ")
                health_checks.append({
                    "topic": "ecology",
                    "status": "stale" if stale else "ok",
                    "msg": f"AQI/PM2.5 · {districts} р-нов · {ts}",
                })
            else:
                health_checks.append({"topic": "ecology", "status": "missing",
                                      "msg": "Нет данных экологии"})
        except Exception:
            health_checks.append({"topic": "ecology", "status": "missing",
                                  "msg": "Ошибка проверки экологии"})

        # ── Камеры фиксации ──────────────────────────────────────────────────
        try:
            from .cameras_cache import get_cameras_meta, is_cameras_stale, count_cameras
            cam_meta = get_cameras_meta()
            if cam_meta.get("last_updated"):
                stale = is_cameras_stale()
                n = cam_meta.get("total_rows", count_cameras())
                ts = str(cam_meta["last_updated"])[:16].replace("T", " ")
                health_checks.append({
                    "topic": "cameras",
                    "status": "stale" if stale else "ok",
                    "msg": f"{n} камер · OSM · {ts}",
                })
            else:
                health_checks.append({"topic": "cameras", "status": "missing",
                                      "msg": "Нет данных о камерах"})
        except Exception:
            health_checks.append({"topic": "cameras", "status": "missing",
                                  "msg": "Ошибка проверки камер"})

        yield _sse({"type": "health", "checks": health_checks})

        # ── Запуск pytest ────────────────────────────────────────────────────
        import importlib.util
        if importlib.util.find_spec("pytest") is None:
            yield _sse({"type": "log", "line": "⚠ pytest не установлен в этом окружении."})
            yield _sse({"type": "log", "line": "Тестирование доступно только в локальной разработке."})
            yield _sse({"type": "log", "line": "Для запуска локально: pip install pytest && python -m pytest tests/"})
            yield _sse({"type": "done", "passed": 0, "failed": 0, "total": 0,
                        "returncode": -1, "failed_lines": [], "no_pytest": True})
            return

        project_root = Path(__file__).parent.parent
        proc = subprocess.Popen(
            [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short", "--no-header", "-q"],
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
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
    with_coords: bool = Query(
        False,
        description=(
            "Обогатить строки результата координатами (_lat, _lon) через 2GIS Geocoder. "
            "Работает только для операций FILTER и TOP_N с адресными данными. "
            "Требует настроенный 2GIS API ключ; без ключа строки возвращаются без изменений."
        ),
    ),
    offset: int = Query(
        0, ge=0,
        description="Смещение строк для пагинации (0-based). Используется с операцией FILTER.",
    ),
    page_size: int = Query(
        20, ge=1, le=200,
        description="Размер страницы для операции FILTER (по умолчанию 20, макс. 200).",
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
            "operation": "UNKNOWN",
        }

    topic = route_result.topic
    plan = make_plan(q, topic)
    # Пагинация: применяем параметры запроса для FILTER-операций
    plan.offset = offset
    if page_size != 20 or plan.limit is None:
        plan.limit = page_size

    # ── Строительство ─────────────────────────────────────────────────────────
    if topic == "construction":
        from .executor import execute_construction
        from .construction_opendata import get_construction_meta, permits_available

        if not permits_available():
            return JSONResponse(
                status_code=503,
                content={
                    "error": "Данные о строительстве не загружены",
                    "hint": "POST /update?topic=construction_permits и POST /update?topic=construction_commissioned",
                },
            )

        result = execute_construction(plan)
        meta = get_construction_meta()
        return {
            "query": q,
            "topic": "construction",
            "operation": plan.operation,
            "district": plan.district,
            "meta": {
                "permits_total": meta.get("permits_total", 0),
                "commissioned_total": meta.get("commissioned_total", 0),
                "active_total": meta.get("active_total", 0),
                "permits_updated": meta.get("permits_updated", ""),
                "commissioned_updated": meta.get("commissioned_updated", ""),
            },
            **result,
        }

    # ── Экология и метеорология ───────────────────────────────────────────────
    if topic == "ecology":
        from .executor import execute_ecology
        from .ecology_cache import (
            is_ecology_stale, get_ecology_meta, upsert_stations, upsert_measurements,
            is_forecast_stale, upsert_forecast,
        )
        from .ecology_fetcher import fetch_all_ecology, fetch_all_forecast

        if is_ecology_stale():
            upsert_stations()
            upsert_measurements(fetch_all_ecology())

        # Прогноз обновляется раз в 6 часов (независимо от измерений)
        if is_forecast_stale():
            upsert_forecast(fetch_all_forecast())

        result = execute_ecology(plan)
        meta = get_ecology_meta()
        # Для ECO_STATUS добавляем риски в ответ
        if plan.operation == "ECO_STATUS":
            from .ecology_cache import query_risks
            result["risks"] = query_risks(district_filter=plan.district)
        return {
            "query": q,
            "topic": topic,
            "topic_name": route_result.name,
            "confidence": round(route_result.confidence, 3),
            "operation": plan.operation,
            "district": plan.district,
            "sub_district": plan.sub_district,
            "ecology_meta": {k: str(v) if not isinstance(v, (int, float, str, bool, type(None))) else v
                             for k, v in meta.items()},
            **result,
        }

    # ── Отключения ЖКХ ───────────────────────────────────────────────────────
    if topic == "power_outages":
        from .executor import execute_power
        from .power_cache import is_power_stale, get_power_meta, upsert_outages
        from .power_scraper import fetch_all_outages

        if is_power_stale():
            upsert_outages(fetch_all_outages())

        result = execute_power(plan)
        # Вычисляем те же фильтры, что использует execute_power,
        # чтобы цифры в шапке совпадали с числами в строках таблицы
        _raw = plan.extra_filters.get("utility", None)
        _uf = "электроснабж" if _raw is None else (_raw or None)
        meta = get_power_meta(utility_filter=_uf, district_filter=plan.district)
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

    # ── Индекс дорожной нагрузки ──────────────────────────────────────────────
    if topic == "traffic_index":
        from .traffic_index import get_traffic_index_with_weather
        ti = get_traffic_index_with_weather()
        return {
            "query":      q,
            "topic":      "traffic_index",
            "topic_name": route_result.name,
            "confidence": round(route_result.confidence, 3),
            "operation":  "TRAFFIC_INDEX",
            **ti,
        }

    # ── Маршруты общественного транспорта (из кэша остановок) ────────────────
    if topic == "transit":
        import re as _re
        from .cache import _get_conn, table_exists as _table_exists
        from .transport_api import DISTRICT_COORDS

        from_district = plan.extra_filters.get("from_district") or ""
        to_district   = plan.extra_filters.get("to_district") or ""

        if not _table_exists("stops"):
            return {
                "query": q, "topic": "transit", "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "TRANSIT_ROUTE",
                "error": "Данные об остановках не загружены",
                "hint": "POST /update?topic=stops",
                "connections": [],
            }

        def _split_routes(marshryt: str) -> list[str]:
            """Извлекает номера маршрутов из строки вида 'Автобус: 23, 36.Маршрутное такси: 4.'"""
            if not marshryt:
                return []
            return _re.findall(r"\b\d+[а-яёa-z]?\b", marshryt)

        conn = _get_conn()
        try:
            def _get_route_stops(kw: str) -> dict[str, list[str]]:
                if not kw:
                    return {}
                rows = conn.execute(
                    "SELECT OstName, Marshryt FROM topic_stops "
                    "WHERE AdrDistr ILIKE ? AND Marshryt IS NOT NULL AND Marshryt != ''",
                    [f"%{kw.split()[0]}%"],
                ).fetchall()
                result: dict[str, list[str]] = {}
                for stop_name, marshryt in rows:
                    for route in _split_routes(marshryt or ""):
                        if route not in result:
                            result[route] = []
                        if stop_name and stop_name not in result[route]:
                            result[route].append(stop_name)
                return result

            from_routes = _get_route_stops(from_district)
            to_routes   = _get_route_stops(to_district)
            common = sorted(set(from_routes) & set(to_routes))
            connections = [
                {"route": r, "from_stops": from_routes[r][:3], "to_stops": to_routes[r][:3]}
                for r in common[:20]
            ]

            from_coords = DISTRICT_COORDS.get(from_district)
            to_coords   = DISTRICT_COORDS.get(to_district)
            hint = None
            if from_coords and to_coords:
                hint = (
                    f"https://2gis.ru/novosibirsk/routeSearch/rsType/publictransport/"
                    f"from/{from_coords[0]},{from_coords[1]}/to/{to_coords[0]},{to_coords[1]}"
                )

            return {
                "query": q, "topic": "transit", "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "TRANSIT_ROUTE",
                "from": from_district, "to": to_district,
                "common_routes_count": len(common),
                "connections": connections,
                "hint": hint,
                "notice": (
                    "⚠️ Данные о маршрутах взяты из открытых данных мэрии Новосибирска "
                    "(opendata.novo-sibirsk.ru) и могут быть неполными или устаревшими. "
                    "Для построения точного маршрута воспользуйтесь приложением 2ГИС или Яндекс.Транспорт."
                ),
                "source": "opendata.novo-sibirsk.ru · остановки наземного транспорта (TTL 24ч)",
            }
        except Exception as e:
            log.error(f"Ошибка /ask transit: {e}")
            return {"query": q, "topic": "transit", "error": str(e), "connections": []}
        finally:
            conn.close()

    # ── Камеры фиксации нарушений ПДД (OSM Overpass) ─────────────────────────
    if topic == "cameras":
        from .cameras_cache import (
            query_cameras, count_cameras, get_cameras_meta,
            upsert_cameras, is_cameras_stale,
        )
        from .cameras_fetcher import fetch_cameras

        if is_cameras_stale():
            fetched = fetch_cameras()
            if fetched:
                upsert_cameras(fetched)

        op = plan.operation
        meta = get_cameras_meta()
        district = plan.district
        total = count_cameras(district_filter=district)

        if op == "COUNT":
            return {
                "query": q,
                "topic": topic,
                "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "COUNT",
                "count": total,
                "rows": [],
                "columns": [],
                "cameras_meta": {
                    "last_updated": str(meta.get("last_updated") or ""),
                    "total_rows": meta.get("total_rows", 0),
                    "source": "OpenStreetMap · Overpass API",
                },
            }
        else:
            lim = plan.limit or 20
            off = plan.offset or 0
            rows = query_cameras(limit=lim, offset=off, district_filter=district)
            return {
                "query": q,
                "topic": topic,
                "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "FILTER",
                "count": total,
                "rows": rows,
                "columns": ["osm_id", "_lat", "_lon", "maxspeed", "name", "direction", "ref", "district"],
                "coords_enriched": True,
                "coords_source": "OpenStreetMap (предзагружены)",
                "cameras_meta": {
                    "last_updated": str(meta.get("last_updated") or ""),
                    "total_rows": meta.get("total_rows", 0),
                    "source": "OpenStreetMap · Overpass API",
                },
            }

    # ── Стандартные темы opendata ─────────────────────────────────────────────
    if not table_exists(topic):
        return {
            "query": q,
            "topic": topic,
            "error": f"Данные не загружены. POST /update?topic={topic}",
        }

    result = execute_plan(plan)

    # ── Геокодирование (опционально) ──────────────────────────────────────────
    if with_coords and plan.operation in ("FILTER", "TOP_N") and result.get("rows"):
        from .geocoder import geocode_rows
        result["rows"] = geocode_rows(result["rows"])
        result["coords_enriched"] = True
        result["coords_source"] = "2GIS Geocoder (кеш + API)"

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


@app.get(
    "/traffic-index",
    tags=["Запросы"],
    summary="Индекс дорожной нагрузки (аналитика)",
    response_description="Синтетический индекс пробок 0–10 с факторами и рекомендациями.",
)
def get_traffic_index() -> dict:
    """
    Синтетический **индекс дорожной нагрузки** для Новосибирска (0 = пусто, 10 = коллапс).

    Алгоритм учитывает:
    - **Время суток** — утренний (07:30–09:30) и вечерний (16:30–19:00) час пик
    - **День недели** — понедельничный эффект (+0.8), пятничный исход (+0.7)
    - **Официальные праздники 2025–2027** — нерабочие дни, предпраздничные укороченные дни
    - **Погоду** — снег (+1.5/+2.5), первый осенний снег (+3.0), дождь (+0.8), гололедица (+2.5)
    - **Городские события** — 1 сентября (+2.0), предновогодняя суета (+2.0)
    - **Экстремальный мороз** (< −20°C) — снижает трафик (машины не заводятся)

    Погодные данные берутся из кэша Open-Meteo (обновляется каждые 15 мин).

    > ⚠️ Реальные данные о пробках отсутствуют — только аналитическая модель.
    """
    from .traffic_index import get_traffic_index_with_weather
    return get_traffic_index_with_weather()


@app.get(
    "/transit",
    tags=["Запросы"],
    summary="Маршруты между районами (открытые данные мэрии)",
    response_description="Общие маршруты наземного транспорта между двумя районами.",
)
def get_transit(
    from_district: str = Query(
        ...,
        description="Район отправления. Например: `Советский район`",
        example="Советский район",
    ),
    to_district: str = Query(
        ...,
        description="Район назначения. Например: `Дзержинский район`",
        example="Дзержинский район",
    ),
) -> dict:
    """
    Находит общие маршруты наземного транспорта между двумя районами Новосибирска.

    Использует данные об остановках из [opendata.novo-sibirsk.ru](http://opendata.novo-sibirsk.ru)
    (тема `stops`, TTL 24ч). **Ключ API не требуется.**

    Для каждого общего маршрута возвращает примеры остановок в районе отправления и назначения.
    Также включает ссылку для построения точного маршрута в 2ГИС.

    > ⚠️ Данные о маршрутах из открытых данных мэрии могут быть неполными.
    > Для точного маршрута используйте 2ГИС или Яндекс.Транспорт.
    """
    import re
    from .cache import _get_conn, table_exists
    from .transport_api import DISTRICT_COORDS

    if not table_exists("stops"):
        return {
            "error": "Данные об остановках не загружены",
            "hint": "POST /update?topic=stops",
            "connections": [],
        }

    def split_routes(marshryt: str) -> list[str]:
        """Извлекает номера маршрутов из строки вида 'Автобус: 23, 36.Маршрутное такси: 4.'"""
        if not marshryt:
            return []
        return re.findall(r"\b\d+[а-яёa-z]?\b", marshryt)

    conn = _get_conn()
    try:
        from_kw = from_district.split()[0]
        to_kw = to_district.split()[0]

        def get_route_stops(kw: str) -> dict[str, list[str]]:
            rows = conn.execute(
                "SELECT OstName, Marshryt FROM topic_stops "
                "WHERE AdrDistr ILIKE ? AND Marshryt IS NOT NULL AND Marshryt != ''",
                [f"%{kw}%"],
            ).fetchall()
            result: dict[str, list[str]] = {}
            for stop_name, marshryt in rows:
                for route in split_routes(marshryt or ""):
                    if route not in result:
                        result[route] = []
                    if stop_name and stop_name not in result[route]:
                        result[route].append(stop_name)
            return result

        from_routes = get_route_stops(from_kw)
        to_routes = get_route_stops(to_kw)
        common = sorted(set(from_routes) & set(to_routes))

        connections = [
            {
                "route": r,
                "from_stops": from_routes[r][:3],
                "to_stops": to_routes[r][:3],
            }
            for r in common[:20]
        ]

        from_coords = DISTRICT_COORDS.get(from_district)
        to_coords = DISTRICT_COORDS.get(to_district)
        hint = None
        if from_coords and to_coords:
            hint = (
                f"https://2gis.ru/novosibirsk/routeSearch/rsType/publictransport/"
                f"from/{from_coords[0]},{from_coords[1]}/to/{to_coords[0]},{to_coords[1]}"
            )

        return {
            "from": from_district,
            "to": to_district,
            "common_routes_count": len(common),
            "connections": connections,
            "hint": hint,
            "notice": (
                "⚠️ Данные о маршрутах взяты из открытых данных мэрии Новосибирска "
                "(opendata.novo-sibirsk.ru) и могут быть неполными или устаревшими. "
                "Для построения точного маршрута воспользуйтесь приложением 2ГИС или Яндекс.Транспорт."
            ),
            "source": "opendata.novo-sibirsk.ru · остановки наземного транспорта (TTL 24ч)",
        }
    except Exception as e:
        log.error(f"Ошибка /transit: {e}")
        return {"error": str(e), "connections": []}
    finally:
        conn.close()


@app.get(
    "/transit/districts",
    tags=["Запросы"],
    summary="Транспортная инфраструктура по районам (без ключа API)",
    response_description="Число остановок наземного транспорта по районам",
)
def get_transit_districts() -> dict:
    """
    Возвращает число остановок наземного пассажирского транспорта по районам
    из кэша [opendata.novo-sibirsk.ru](http://opendata.novo-sibirsk.ru).

    **Ключ API не требуется.** Данные берутся из темы `stops` (TTL 24ч).

    Для обновления данных: `POST /update?topic=stops`
    """
    from .cache import _get_conn, table_exists

    if not table_exists("stops"):
        return {
            "error": "Данные об остановках не загружены",
            "hint": "POST /update?topic=stops",
            "rows": [],
            "total_stops": 0,
            "count": 0,
        }

    conn = _get_conn()
    try:
        cursor = conn.execute("""
            SELECT
                AdrDistr AS district,
                COUNT(*) AS stops_count
            FROM topic_stops
            WHERE AdrDistr IS NOT NULL AND AdrDistr != ''
            GROUP BY AdrDistr
            ORDER BY stops_count DESC
        """)
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        total = sum(r["stops_count"] for r in rows)
        return {
            "operation": "TRANSIT_DISTRICTS",
            "count": len(rows),
            "total_stops": total,
            "rows": rows,
            "columns": cols,
            "source": "opendata.novo-sibirsk.ru · остановки наземного транспорта",
        }
    except Exception as e:
        log.error(f"Ошибка /transit/districts: {e}")
        return {"error": str(e), "rows": [], "total_stops": 0, "count": 0}
    finally:
        conn.close()


@app.get(
    "/twogis/geocode",
    tags=["2GIS"],
    summary="Геокодировать адрес (адрес → координаты)",
    response_description="Координаты объекта или сообщение о недоступности",
)
def get_geocode(
    q: str = Query(..., description="Адрес для геокодирования. Например: `ул. Красный проспект, 25`"),
    city: str = Query("Новосибирск", description="Город (префикс к запросу)"),
) -> dict:
    """
    Конвертирует адресную строку в координаты через 2GIS Geocoder API.

    - Результаты кешируются в DuckDB (повторные запросы мгновенны, ключ не нужен).
    - Если ключ не задан и адрес не в кеше → `available: false`.
    - Используйте `GET /twogis/geocache-stats` чтобы увидеть размер кеша.
    """
    from .geocoder import geocode

    key = _get_twogis_key()
    result = geocode(q, city)

    if result is None:
        if not key:
            return {"available": False, "reason": "2GIS ключ не задан. POST /twogis/key"}
        return {"available": True, "found": False, "query": q}

    return {
        "available": True,
        "found": True,
        "query": q,
        "lat": result["lat"],
        "lon": result["lon"],
        "full_name": result["full_name"],
        "source": result["source"],
    }


@app.get(
    "/twogis/geocache-stats",
    tags=["2GIS"],
    summary="Статистика кеша геокодирования",
)
def get_geocache_stats() -> dict:
    """Показывает количество адресов, сохранённых в кеше геокодирования."""
    from .geocoder import geocode_stats
    return geocode_stats()


@app.get(
    "/twogis/key",
    tags=["2GIS"],
    summary="Текущий ключ 2GIS API",
    response_description="Ключ (маскированный), источник и статус",
)
def get_twogis_key_info() -> dict:
    """
    Возвращает информацию о текущем ключе 2GIS API.

    | Поле | Описание |
    |---|---|
    | `is_set` | `true` если ключ задан |
    | `masked` | Маскированное значение вида `2ea1a391...9092` |
    | `source` | Откуда взят ключ: `env` / `file` / `unset` |

    Для проверки валидности используйте `GET /twogis/validate`.
    """
    key = _get_twogis_key()
    if os.environ.get("TWOGIS_API_KEY", "").strip():
        source = "env"
    elif _load_api_keys().get("twogis_key", "").strip():
        source = "file"
    else:
        source = "unset"
    masked = f"{key[:8]}...{key[-4:]}" if key and len(key) > 12 else (key or "")
    return {
        "is_set": bool(key),
        "masked": masked,
        "source": source,
    }


@app.post(
    "/twogis/key",
    tags=["2GIS"],
    summary="Сохранить ключ 2GIS API",
    response_description="Подтверждение сохранения",
)
def set_twogis_key(
    key: str = Query(..., description="API ключ 2GIS. Получить: platform.2gis.ru"),
) -> dict:
    """
    Сохраняет ключ 2GIS API в `data/api_keys.json` и сразу применяет его в текущем процессе.

    - Если переменная окружения `TWOGIS_API_KEY` задана, она имеет приоритет над файлом.
    - После сохранения вызовите `GET /twogis/validate` для проверки.
    """
    key = key.strip()
    if not key:
        return {"ok": False, "error": "Ключ не может быть пустым"}
    keys = _load_api_keys()
    keys["twogis_key"] = key
    _save_api_keys(keys)
    os.environ["TWOGIS_API_KEY"] = key
    masked = f"{key[:8]}...{key[-4:]}" if len(key) > 12 else key
    return {"ok": True, "masked": masked, "source": "file"}


@app.get(
    "/twogis/validate",
    tags=["2GIS"],
    summary="Проверить валидность ключа 2GIS API",
    response_description="Результат проверки через Geocoder API",
)
def validate_twogis_key(
    key: str | None = Query(
        None,
        description="Ключ для проверки. Если не указан — используется текущий сохранённый.",
    ),
) -> dict:
    """
    Проверяет ключ 2GIS, выполняя тестовый запрос к Geocoder API
    (`catalog.api.2gis.com/3.0/items/geocode?q=Новосибирск`).

    | `valid` | Значение |
    |---|---|
    | `true` | Ключ рабочий, API отвечает |
    | `false` | Ключ неверный, истёк или сеть недоступна |
    """
    import requests as _req

    check_key = key.strip() if key else _get_twogis_key()
    if not check_key:
        return {"valid": False, "error": "Ключ не задан. Сохраните его через POST /twogis/key"}
    masked = f"{check_key[:8]}...{check_key[-4:]}" if len(check_key) > 12 else check_key
    try:
        resp = _req.get(
            "https://catalog.api.2gis.com/3.0/items/geocode",
            params={"q": "Новосибирск", "fields": "items.point", "key": check_key},
            timeout=10,
        )
        if resp.status_code == 200:
            total = resp.json().get("result", {}).get("total", 0)
            return {"valid": True, "masked": masked, "status_code": 200, "geocoder_results": total}
        elif resp.status_code == 403:
            return {"valid": False, "masked": masked, "error": "Ключ неверный или истёк", "status_code": 403}
        else:
            return {"valid": False, "masked": masked, "error": f"HTTP {resp.status_code}", "status_code": resp.status_code}
    except Exception as e:
        return {"valid": False, "masked": masked, "error": str(e)}


@app.get("/mapgl-key", include_in_schema=False)
def get_mapgl_key() -> dict:
    """Возвращает ключ 2GIS для инициализации MapGL JS на фронтенде."""
    key = _get_twogis_key()
    return {"key": key or "", "available": bool(key)}


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
            "`libraries`, `pharmacies`, `sport_grounds`, `sport_orgs`, `culture`"
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

    > **Камеры фиксации** (`cameras`) имеют отдельный эндпоинт: `POST /cameras/update`
    > (источник — OpenStreetMap Overpass API, TTL 7 дней).
    """
    from .cli import _do_update
    from .registry import list_topics

    topics_to_update = [topic] if topic else list_topics()
    results = {}
    for t in topics_to_update:
        rows = _do_update(t)
        results[t] = {"rows": rows, "success": rows > 0}

    return {"updated": results}


# ── Cameras endpoints ─────────────────────────────────────────────────────────

@app.get(
    "/cameras",
    tags=["Камеры"],
    summary="Список камер фиксации нарушений ПДД",
    response_description="Массив камер с координатами (_lat, _lon) и мета-информацией",
)
def get_cameras(
    limit: int = Query(60, ge=1, le=200, description="Максимум записей в ответе"),
    district: str | None = Query(None, description="Фильтр по району (например 'Советский')"),
) -> dict:
    """
    Возвращает список стационарных камер фиксации нарушений ПДД в Новосибирске.

    Данные берутся из кеша OSM (Overpass API, тег `highway=speed_camera`).
    При первом запросе или по истечении TTL (7 дней) кеш обновляется автоматически.

    ### Поля каждой камеры

    | Поле | Тип | Описание |
    |---|---|---|
    | `osm_id` | string | ID объекта в OpenStreetMap |
    | `_lat` | float | Широта |
    | `_lon` | float | Долгота |
    | `maxspeed` | string | Ограничение скорости (например `60`) |
    | `name` | string | Название камеры (если задано в OSM) |
    | `direction` | string | Направление съёмки в градусах (если задано) |
    | `ref` | string | Номер / ссылка (если задано) |
    | `district` | string | Район города (вычисляется по координатам) |

    **Лицензия:** данные OpenStreetMap, ODbL — [openstreetmap.org/copyright](https://www.openstreetmap.org/copyright)

    Эквивалентно запросу: `GET /ask?q=камеры+видеофиксации`
    """
    from .cameras_cache import query_cameras, count_cameras, get_cameras_meta, upsert_cameras, is_cameras_stale
    from .cameras_fetcher import fetch_cameras

    if is_cameras_stale():
        fetched = fetch_cameras()
        if fetched:
            upsert_cameras(fetched)

    rows = query_cameras(limit=limit, district_filter=district)
    meta = get_cameras_meta()
    return {
        "operation": "FILTER",
        "count": count_cameras(district_filter=district),
        "rows": rows,
        "columns": ["osm_id", "_lat", "_lon", "maxspeed", "name", "direction", "ref", "district"],
        "coords_enriched": True,
        "coords_source": "OpenStreetMap (предзагружены)",
        "cameras_meta": {
            "last_updated": str(meta.get("last_updated") or ""),
            "total_rows": meta.get("total_rows", 0),
            "source": "OpenStreetMap · Overpass API · highway=speed_camera",
            "bbox": "54.70,82.60,55.25,83.40 (Новосибирск)",
            "license": "ODbL · openstreetmap.org/copyright",
        },
    }


@app.post(
    "/cameras/update",
    tags=["Камеры"],
    summary="Обновить данные о камерах фиксации нарушений (OSM)",
    response_description="Статус обновления: rows и success",
)
def post_cameras_update() -> dict:
    """
    Принудительно обновляет данные о стационарных камерах фиксации нарушений ПДД
    из OpenStreetMap через Overpass API.

    TTL: 7 дней. При запросах через `/ask?q=камеры` обновление происходит автоматически.

    **Источник:** OpenStreetMap, лицензия ODbL (openstreetmap.org/copyright).
    """
    from .cameras_fetcher import fetch_cameras
    from .cameras_cache import upsert_cameras, count_cameras
    cameras = fetch_cameras()
    if not cameras:
        # Overpass API вернул пустой ответ — оставляем старые данные без изменений
        existing = count_cameras()
        return {"updated": {"cameras": {
            "rows": existing,
            "success": existing > 0,
            "warning": "Overpass API недоступен — возвращены кешированные данные",
        }}}
    rows = upsert_cameras(cameras)
    return {"updated": {"cameras": {"rows": rows, "success": rows > 0}}}


# ── Ecology endpoints ─────────────────────────────────────────────────────────

def _ecology_auto_update() -> None:
    """Обновляет данные экологии и прогноза если TTL истёк."""
    from .ecology_cache import (
        is_ecology_stale, upsert_stations, upsert_measurements,
        is_forecast_stale, upsert_forecast,
    )
    from .ecology_fetcher import fetch_all_ecology, fetch_all_forecast
    if is_ecology_stale():
        upsert_stations()
        upsert_measurements(fetch_all_ecology())
    if is_forecast_stale():
        upsert_forecast(fetch_all_forecast())


@app.get(
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
    from .ecology_cache import query_current, get_ecology_meta
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


@app.get(
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
    from .ecology_cache import query_pdk_exceedances
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


@app.get(
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
    from .ecology_cache import query_history
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


@app.post(
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
    from .ecology_fetcher import fetch_all_ecology
    from .ecology_cache import upsert_stations, upsert_measurements, get_ecology_meta
    import os

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


@app.post(
    "/power/update",
    tags=["Управление"],
    summary="Обновить данные об отключениях ЖКХ",
    response_description="Статус обновления: количество загруженных записей",
)
def post_power_update() -> dict:
    """
    Принудительно загружает актуальные данные об отключениях ЖКХ с
    [051.novo-sibirsk.ru](http://051.novo-sibirsk.ru).

    В штатном режиме обновление происходит автоматически при запросах через `/ask`
    (тема `power_outages`) если TTL (30 мин) истёк.

    Загружает все типы ресурсов: электроснабжение, теплоснабжение, горячая вода,
    холодная вода, газоснабжение.
    """
    from .power_scraper import fetch_all_outages
    from .power_cache import upsert_outages, get_power_meta

    records = fetch_all_outages()
    count = upsert_outages(records)
    meta = get_power_meta()
    return {
        "success": count > 0,
        "records_loaded": count,
        "active_houses": meta.get("active_houses", 0),
        "planned_houses": meta.get("planned_houses", 0),
        "last_scraped": meta.get("last_scraped", ""),
        "source": "051.novo-sibirsk.ru",
    }


@app.get(
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
    from .ecology_cache import query_risks, get_ecology_meta
    risks = query_risks(district_filter=district)
    meta = get_ecology_meta()
    return {
        "operation": "ECO_RISKS",
        "district": district,
        "count": len(risks),
        "risks": risks,
        "ecology_meta": meta,
    }
