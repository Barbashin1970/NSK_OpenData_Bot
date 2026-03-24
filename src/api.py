"""FastAPI HTTP API для NSK OpenData Bot (bot serve).

Этот файл — точка входа: создаёт FastAPI app, подключает middleware, startup events
и роутеры. Бизнес-логика эндпоинтов живёт в src/routes/*.py.
"""

import json
import logging
import os
import re as _re
import subprocess
import sys
from pathlib import Path

log = logging.getLogger(__name__)

try:
    from fastapi import FastAPI, Request
    from fastapi.responses import HTMLResponse, StreamingResponse
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.staticfiles import StaticFiles
except ImportError:
    raise ImportError("Установите fastapi: pip install fastapi uvicorn")

from .city_config import (
    get_city_name, get_city_id, get_districts,
    get_feature, get_opendata_base_url,
)

_STATIC = Path(__file__).parent / "static"
_API_KEYS_FILE = Path(__file__).parent.parent / "data" / "api_keys.json"


def _load_api_keys() -> dict:
    try:
        if _API_KEYS_FILE.exists():
            return json.loads(_API_KEYS_FILE.read_text("utf-8"))
    except Exception:
        pass
    return {}


def _build_api_description() -> str:
    city_gen  = get_city_name(case="genitive")
    districts = get_districts()
    main_districts = [k for k in districts if "район" in k]
    district_line = " · ".join(k.replace(" район", "") for k in main_districts)
    district_count = len(main_districts)
    return f"""
Естественно-языковой интерфейс к открытым данным мэрии {city_gen}.

## Источники данных

| Источник | TTL | Что содержит |
|---|---|---|
| [opendata.novo-sibirsk.ru](http://opendata.novo-sibirsk.ru) | 24 ч | Парковки, школы, аптеки, библиотеки и др. |
| [051.novo-sibirsk.ru](http://051.novo-sibirsk.ru) | 30 мин | Отключения ЖКХ: электро, тепло, вода, газ |
| [Open-Meteo](https://open-meteo.com) | 15 мин | PM2.5, PM10, AQI, погода по 11 точкам (бесплатно) |
| CityAir API | 15 мин | Телеметрия физических датчиков (требует `CITYAIR_API_KEY`) |
| [2GIS Public Transport](https://dev.2gis.com/api) | real-time | Маршруты общественного транспорта (требует `TWOGIS_API_KEY`, данные не сохраняются) |
| [OpenStreetMap Overpass API](https://overpass-api.de) | 7 дн | Стационарные камеры фиксации нарушений ПДД · лицензия ODbL |
| [OpenStreetMap Overpass API](https://overpass-api.de) | 72 ч | Медицинские учреждения (больницы, поликлиники) · лицензия ODbL |

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
| `medical` | Медицинские учреждения (больницы, поликлиники) | ~100+ (OSM) |
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

## Районы {city_gen} и прилегающие территории

{district_line}

Также поддерживается: **Кольцово** (наукоград, отдельная точка мониторинга погоды и качества воздуха).
Запросы к открытым данным города (школы, парковки и т.д.) для Кольцово не применимы —
наукоград находится вне {district_count} административных районов {city_gen}.

"""

_API_DESCRIPTION = _build_api_description()

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
            f"Качество воздуха (PM2.5, PM10, NO2, AQI) и метеорология по {len([k for k in get_districts() if 'район' in k])} районам {get_city_name(case='genitive')}. "
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
            f"Стационарные камеры фиксации нарушений ПДД в {get_city_name(case='prepositional')}. "
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
    version="1.2.0",
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


@app.middleware("http")
async def _no_cache_static(request: Request, call_next):
    """Запрещаем браузеру кэшировать статику — всегда свежий JS/CSS при разработке."""
    response = await call_next(request)
    if request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store"
    return response


# ── .env loader ───────────────────────────────────────────────────────────────

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
        logging.getLogger(__name__).warning("Не удалось загрузить .env: %s", e)


# ── Startup events ───────────────────────────────────────────────────────────

@app.on_event("startup")
def _load_saved_api_keys() -> None:
    """При старте: загружает ключи из .env, затем из data/api_keys.json (если ENV не задан)."""
    _load_dotenv()
    if not os.environ.get("TWOGIS_API_KEY", "").strip():
        saved = _load_api_keys().get("twogis_key", "").strip()
        if saved:
            os.environ["TWOGIS_API_KEY"] = saved


@app.on_event("startup")
def _seed_ecology_history() -> None:
    """При старте заполняет ecology_daily_archive заглушками за последние 20 дней."""
    try:
        from .ecology_cache import seed_history_placeholder
        seed_history_placeholder(days=20, temp_c=-10.0)
    except Exception as e:
        logging.getLogger(__name__).warning(f"seed_ecology_history: {e}")


@app.on_event("startup")
async def _geocode_metro_stations() -> None:
    """Фоновое геокодирование станций метро через 2GIS при старте."""
    import asyncio

    async def _run():
        await asyncio.sleep(5)
        try:
            from .executor import _geocode_metro_bg
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _geocode_metro_bg)
            logging.getLogger(__name__).info("metro geocoding: готово")
        except Exception as e:
            logging.getLogger(__name__).warning(f"metro geocoding startup: {e}")

    asyncio.create_task(_run())


@app.on_event("startup")
async def _preload_medical() -> None:
    """Фоновая предзагрузка медучреждений (OSM) при старте."""
    import asyncio

    async def _run():
        await asyncio.sleep(30)
        try:
            from .medical_cache import is_medical_stale, upsert_medical
            from .medical_fetcher import fetch_medical
            if is_medical_stale():
                data = fetch_medical()
                if data:
                    upsert_medical(data)
                    logging.getLogger(__name__).info("medical preload: загружено %d объектов", len(data))
        except Exception as e:
            logging.getLogger(__name__).warning("medical preload startup: %s", e)

    asyncio.create_task(_run())


@app.on_event("startup")
async def _start_background_preloader() -> None:
    """Фоновая загрузка всех тем opendata после старта сервера."""
    import asyncio
    from .updater import preload_all_async, periodic_refresh_loop
    asyncio.create_task(preload_all_async(delay_start=15.0))
    asyncio.create_task(periodic_refresh_loop())


# ── Кастомный Swagger UI ─────────────────────────────────────────────────────
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
  body { padding-top: 44px !important; }
  /* Hide nav bar when loaded inside an iframe (e.g. Studio API tab) */
  @media all {
    body.in-iframe #nsk-nav { display: none !important; }
    body.in-iframe { padding-top: 0 !important; }
  }

</style>

<script>if (window !== window.top) document.body.classList.add('in-iframe');</script>

<div id="nsk-nav">
  <a href="/" class="back">← На главную</a>
  <span class="sep">|</span>
  <span class="title">NSK OpenData Bot</span>
  <span class="sub">API Документация</span>
  <span class="badge">v1.2.0</span>
</div>

<script>
/* Testing moved to /studio#testing */
// ── Dev password modal ────────────────────────────────────────────────────
const NSKDev = (() => {
  function _modal(html) {
    let el = document.getElementById('nsk-dev-modal');
    if (!el) {
      el = document.createElement('div');
      el.id = 'nsk-dev-modal';
      el.style.cssText = 'position:fixed;inset:0;z-index:99999;background:rgba(0,0,0,.55);display:flex;align-items:center;justify-content:center;';
      document.body.appendChild(el);
    }
    el.innerHTML = html;
    el.style.display = 'flex';
    el.addEventListener('click', e => { if(e.target===el) el.style.display='none'; });
  }
  function _close() {
    const el = document.getElementById('nsk-dev-modal');
    if (el) el.style.display = 'none';
  }
  async function changePassword() {
    _modal(`<div style="background:#1e293b;border:1px solid #334155;border-radius:12px;padding:28px 32px;width:360px;color:#e2e8f0;font-family:system-ui,sans-serif;">
      <div style="font-weight:700;font-size:15px;margin-bottom:16px;">🔑 Изменить пароль разработчика</div>
      <label style="font-size:12px;color:#94a3b8;">Текущий пароль</label>
      <input id="dp-old" type="password" placeholder="Текущий пароль"
        style="display:block;width:100%;margin:4px 0 12px;padding:8px 10px;background:#0f172a;border:1px solid #334155;border-radius:8px;color:#e2e8f0;font-size:13px;box-sizing:border-box;"/>
      <label style="font-size:12px;color:#94a3b8;">Новый пароль</label>
      <input id="dp-new" type="password" placeholder="Новый пароль"
        style="display:block;width:100%;margin:4px 0 20px;padding:8px 10px;background:#0f172a;border:1px solid #334155;border-radius:8px;color:#e2e8f0;font-size:13px;box-sizing:border-box;"/>
      <div style="display:flex;gap:10px;">
        <button onclick="NSKDev._submitChange()" style="flex:1;padding:9px;background:#2563eb;border:none;border-radius:8px;color:#fff;font-weight:600;cursor:pointer;font-size:13px;">Изменить</button>
        <button onclick="NSKDev._close()" style="padding:9px 16px;background:#334155;border:none;border-radius:8px;color:#94a3b8;cursor:pointer;font-size:13px;">Отмена</button>
      </div>
      <div id="dp-msg" style="margin-top:10px;font-size:12px;min-height:16px;"></div>
    </div>`);
    setTimeout(() => document.getElementById('dp-old')?.focus(), 50);
  }
  async function _submitChange() {
    const oldP = document.getElementById('dp-old')?.value || '';
    const newP = document.getElementById('dp-new')?.value || '';
    const msg  = document.getElementById('dp-msg');
    if (!oldP || !newP) { msg.style.color='#f87171'; msg.textContent='Заполните оба поля.'; return; }
    if (newP.length < 6) { msg.style.color='#f87171'; msg.textContent='Новый пароль слишком короткий (мин. 6 символов).'; return; }
    msg.style.color='#94a3b8'; msg.textContent='Проверяю…';
    const r = await fetch('/dev-password', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({old_password: oldP, new_password: newP})
    });
    const d = await r.json();
    if (d.success) {
      msg.style.color='#4ade80'; msg.textContent='Пароль успешно изменён.';
      setTimeout(_close, 1500);
    } else {
      msg.style.color='#f87171'; msg.textContent = d.detail || 'Неверный пароль.';
    }
  }
  return { changePassword, _submitChange, _close };
})();
</script>
"""


# ── /run-tests SSE endpoint ──────────────────────────────────────────────────

@app.get("/run-tests", include_in_schema=False)
def run_tests():
    """SSE-стрим: запускает pytest и отдаёт прогресс + проверку здоровья данных."""
    from .registry import load_registry
    from .fetcher import load_meta, is_stale
    from .cache import table_exists

    def _sse(obj: dict) -> str:
        return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

    def generate():
        from .power_cache import get_power_meta

        _has_csv = get_feature("opendata_csv_enabled", False)
        _has_power = bool(get_feature("power_outages_url", ""))

        registry = load_registry()
        meta = load_meta()
        health_checks = []

        if _has_csv:
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

        if _has_power:
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

        try:
            from .medical_cache import get_medical_meta, is_medical_stale, count_medical
            med_meta = get_medical_meta()
            if med_meta.get("last_updated"):
                stale = is_medical_stale()
                n = med_meta.get("total_rows", count_medical())
                ts = str(med_meta["last_updated"])[:16].replace("T", " ")
                health_checks.append({
                    "topic": "medical",
                    "status": "stale" if stale else "ok",
                    "msg": f"{n} медучреждений · OSM · {ts}",
                })
            else:
                health_checks.append({"topic": "medical", "status": "missing",
                                      "msg": "Нет данных о медучреждениях"})
        except Exception:
            health_checks.append({"topic": "medical", "status": "missing",
                                  "msg": "Ошибка проверки медучреждений"})

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

        import importlib.util
        if importlib.util.find_spec("pytest") is None:
            yield _sse({"type": "log", "line": "⚠ pytest не установлен в этом окружении."})
            yield _sse({"type": "log", "line": "Тестирование доступно только в локальной разработке."})
            yield _sse({"type": "log", "line": "Для запуска локально: pip install pytest && python -m pytest tests/"})
            yield _sse({"type": "done", "passed": 0, "failed": 0, "total": 0,
                        "returncode": -1, "failed_lines": [], "no_pytest": True})
            return

        project_root = Path(__file__).parent.parent
        test_env = {**os.environ, "CITY_PROFILE": "city_profile"}
        proc = subprocess.Popen(
            [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short", "--no-header"],
            cwd=str(project_root),
            env=test_env,
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
        skipped = 0
        failed_lines: list[str] = []

        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if not line:
                continue

            m = _re.search(r"collected (\d+) item", line)
            if m:
                total = int(m.group(1))
                yield _sse({"type": "start", "total": total})
                continue

            if _re.search(r"\s(PASSED|FAILED|ERROR)(\s|$)", line):
                done += 1
                if "PASSED" in line:
                    passed += 1
                    status = "passed"
                else:
                    failed += 1
                    status = "failed"
                    failed_lines.append(line)
                pct = int(done / total * 100) if total > 0 else 0
                short = _re.sub(r"\s+(PASSED|FAILED|ERROR).*$", "", _re.sub(r"^.*::", "", line))
                yield _sse({"type": "progress", "done": done, "total": total,
                            "pct": pct, "status": status, "short": short, "line": line})
            else:
                m_fin = _re.search(r"\b(\d+)\s+passed.*\bin\s+[\d.]+s", line)
                if m_fin:
                    passed = int(m_fin.group(1))
                    mf = _re.search(r"\b(\d+)\s+failed", line)
                    ms = _re.search(r"\b(\d+)\s+skipped", line)
                    if mf:
                        failed = int(mf.group(1))
                    if ms:
                        skipped = int(ms.group(1))
                    if not total:
                        total = passed + (int(mf.group(1)) if mf else 0)
                yield _sse({"type": "log", "line": line})

        proc.wait()
        yield _sse({
            "type": "done",
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "total": passed + failed,
            "returncode": proc.returncode,
            "failed_lines": failed_lines,
        })

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.get("/docs", include_in_schema=False)
def custom_swagger_ui() -> HTMLResponse:
    """Swagger UI с навигационной панелью и кнопкой возврата на главную страницу."""
    from fastapi.openapi.docs import get_swagger_ui_html
    html_resp = get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="NSK OpenData Bot — API",
        swagger_ui_parameters={
            "defaultModelsExpandDepth": -1,
            "docExpansion": "list",
            "tryItOutEnabled": True,
            "displayRequestDuration": True,
            "filter": True,
            "persistAuthorization": True,
        },
    )
    html = html_resp.body.decode("utf-8")
    html = html.replace("<body>", f"<body>{_NAV_BAR_HTML}", 1)
    return HTMLResponse(html)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def get_ui() -> HTMLResponse:
    """Веб-интерфейс бота."""
    html_file = _STATIC / "index.html"
    if not html_file.exists():
        return HTMLResponse("<h1>Web UI not found</h1><p>Place index.html in src/static/</p>")
    content = html_file.read_text(encoding="utf-8")
    return HTMLResponse(content, headers={"Cache-Control": "no-store"})


# ── Include routers ──────────────────────────────────────────────────────────

from .routes.data import router as data_router
from .routes.ecology import router as ecology_router
from .routes.transport import router as transport_router
from .routes.cameras import router as cameras_router
from .routes.medical import router as medical_router
from .routes.twogis import router as twogis_router
from .routes.ciinsu import router as ciinsu_router
from .routes.studio import router as studio_router
from .routes.admin import router as admin_router
from .routes.presenter import router as presenter_router

app.include_router(data_router)
app.include_router(ecology_router)
app.include_router(transport_router)
app.include_router(cameras_router)
app.include_router(medical_router)
app.include_router(twogis_router)
app.include_router(ciinsu_router)
app.include_router(studio_router)
app.include_router(admin_router)
app.include_router(presenter_router)


# ── Статические файлы (tailwind.css, иконки и т.д.) ─────────────────────────
if _STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")
