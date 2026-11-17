#!/usr/bin/env python3
"""Демо-копия лендинга СИМА для GitHub Pages: страница и снимок ответов API.

Railway из российских сетей без VPN не открывается, а github.io открывается. Pages раздаёт
только файлы, поэтому сюда идёт копия src/static/index.html с прослойкой, подменяющей fetch:
запрос к API от корня → файл снимка api/<ключ>.json. Снимок забирается с Railway при сборке
(в GitHub Actions — раз в час). Страницы и действия управления выключены. Сам index.html
не меняется — его отдаёт Railway. Разбор: docs/PAGES-DEMO-RAZBOR.md.

    python3 scripts/dlya_pages.py --osnova /NSK_OpenData_Bot --vyhod _pages \\
        --api https://nsk-opendata-bot.up.railway.app

Только стандартная библиотека. В конце — самопроверка: адрес от корня без основы, нет
важного снимка или прослойки — остановка (прошлая публикация на Pages остаётся).
Локально на macOS с Python с python.org: SSL_CERT_FILE="$(python3 -m certifi)" python3 …
"""

import argparse
import json
import re
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, unquote, urlsplit
from urllib.request import Request, urlopen

KOREN = Path(__file__).resolve().parent.parent
STATIC = KOREN / "src" / "static"

# Страницы управления и действия, которые в демо не работают (просьба заказчика 2026-11-17).
# Тот же список — в прослойке (MANAGE): ссылки, window.open, fetch.
UPRAVLENIE = ("studio", "tasks", "presentation", "display", "news-editor", "dev-auth", "admin",
              "api/tasks", "api/backup", "api/set-city", "feedback", "update", "ecology/update",
              "power/update", "mapgl-key", "twogis")
SSYLKA_UPRAVLENIYA = re.compile(r'href="/(studio|tasks|presentation|display|news-editor)([^"]*)"')
ATRIBUT = re.compile(r'\b(href|src|srcset|action|poster)="(/[^"]*)"')
# Без этих снимков демо бессмысленно — сборка останавливается, прошлая публикация остаётся
VAZHNYE = ("/api/city-config", "/power/forecast", "/ecology/status",
           "/ask?q=" + quote("отключения ЖКХ сейчас", safe=""))
PERIODY = (30, 365)          # _PERIOD_OPTS в index.html: 30 дней и «всё время» (_PERIOD_ALL)
PAGE_SIZE = 20               # _pageSize в index.html
FORECAST_RAJON = {"Академгородок": "Советский"}   # _forecastDistrictParam в index.html
# Значения по умолчанию сервера (src/routes/data.py, /ask): с ними и без них ответ один и тот же.
# ask() шлёт /ask?q=…, листалка — /ask?q=…&offset=0&page_size=20; ключ снимка у них общий.
# Тот же словарь уходит в прослойку (S.defaults).
UMOLCHANIYA = {"/ask": {"offset": "0", "page_size": str(PAGE_SIZE)}}


# ── Ключ снимка: то же самое считает прослойка в браузере (см. SHIM) ──────────────
def kanon(adres: str) -> str:
    """Путь и параметры — раскодированные, параметры отсортированы, значения по умолчанию
    сервера отброшены."""
    chasti = urlsplit(adres)
    put = unquote(chasti.path)
    lishnie = UMOLCHANIYA.get(put, {})
    par = sorted((k, v) for k, v in parse_qsl(chasti.query, keep_blank_values=True) if lishnie.get(k) != v)
    return put + ("?" + "&".join(f"{k}={v}" for k, v in par) if par else "")


def fnv1a(s: str) -> str:
    h = 0x811C9DC5
    for b in s.encode("utf-8"):
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return f"{h:08x}"


def fajl_snimka(adres: str) -> str:
    return f"api/{fnv1a(kanon(adres))}.json"


# ── Список адресов снимка — из самого index.html, чтобы не разойтись со страницей ──
def zaprosy_iz_stranicy(html: str, rajony: list[dict]) -> list[str]:
    """Готовые запросы для /ask: литералы, шаблоны с районом, чипы ролей."""
    gotovye: set[str] = set()
    # в обеих кавычках: набор примеров ролей записан как {q: "…"} — без двойных кавычек
    # 25 из 43 примеров не попадали в снимок (проверка живого сайта 2026-11-17)
    for pat in (r"""askNow\((['"])([^'"$`\\]+)\1\)""", r"""setQuery\((['"])([^'"$`\\]+)\1\)""",
                r"""\bq:\s*(['"])([^'"$`\\]+)\1"""):
        gotovye |= {m[1] for m in re.findall(pat, html)}
    # подстановки чипов {D1}, {CITY}… — значения Новосибирска из _resolveChips(...) в коде страницы
    m = re.search(r"let ROLE_CHIPS = _resolveChips\(\s*\{(.*?)\}", html, re.S)
    podst = dict(re.findall(r"([A-Z1-9_]+):\s*'([^']*)'", m.group(1))) if m else {}
    gotovye = {re.sub(r"\{([A-Z1-9_]+)\}", lambda x: podst.get(x.group(1), ""), q).strip() for q in gotovye}
    # шаблоны с ${dShort} — для каждого района
    kratkie = [r["label"] for r in rajony if r.get("val")]
    for shablon in set(re.findall(r"`([^`]*\$\{dShort\}[^`]*)`", html)):
        if shablon.count("${") != 1:
            continue
        for k in kratkie:
            gotovye.add(shablon.replace("${dShort}", k))
    return sorted(q for q in gotovye if q and len(q) < 120)


def spisok_adresov(html: str, konfig: dict) -> list[str]:
    gorod = konfig.get("city_id") or "novosibirsk"
    rajony = [r for r in (konfig.get("districts") or []) if r.get("val")]
    razdely_cii = re.findall(r"\{\s*key:\s*'([a-z]+)',\s*icon:", html.split("const _CII_SECTIONS", 1)[-1][:2000])
    adresa = [
        "/api/city-config", "/api/available-cities", "/topics", "/api/vocabulary", "/api/version", "/api/version/check",
        "/ecology/status", "/ecology/risks", "/traffic-index", "/transit/districts", "/power/forecast",
        f"/budget/{gorod}", "/budget-compare", f"/emissions/{gorod}", "/custom-reports", "/ciinsu/news",
        "/ecology/aqi-exceedances?days=30&aqi_threshold=40",
        "/ask?q=" + quote("отключения ЖКХ сейчас", safe=""),       # табло: без параметров страницы
    ] + [f"/ciinsu?section={k}" for k in razdely_cii]
    for d in PERIODY:
        adresa += [f"/power/history?days={d}&group_by=day", f"/power/history?days={d}&group_by=district",
                   f"/power/efficiency?days={d}", f"/ecology/history?days={d}",
                   f"/ecology/rating-districts?days={d}"]
    for r in rajony:
        v = quote(r["val"], safe="")
        adresa.append("/power/forecast?district=" + quote(FORECAST_RAJON.get(r["val"], r["val"]), safe=""))
        adresa.append(f"/ecology/aqi-exceedances?days=30&aqi_threshold=40&district={v}")
        for d in PERIODY:
            adresa += [f"/power/history?days={d}&group_by=day&district={v}", f"/ecology/history?days={d}&district={v}"]
    for q in zaprosy_iz_stranicy(html, rajony):
        adresa.append(f"/ask?q={quote(q, safe='')}&offset=0&page_size={PAGE_SIZE}")
    # без дублей по ключу, в исходном порядке
    vidannye, itog = set(), []
    for a in adresa:
        if kanon(a) not in vidannye:
            vidannye.add(kanon(a))
            itog.append(a)
    return itog


def dannye_2gis(x) -> bool:
    """Ответ с данными API 2ГИС (маршруты, справочник). Правила 2ГИС разрешают только
    проброс в реальном времени, без хранения, — в снимок такой ответ не идёт. Признак —
    поле source с «2GIS» (src/transport_api.py: «…real-time, данные не сохраняются»)."""
    if isinstance(x, dict):
        return any((k == "source" and isinstance(v, str) and re.search(r"2gis|2гис", v, re.I) is not None)
                   or dannye_2gis(v) for k, v in x.items())
    if isinstance(x, list):
        return any(dannye_2gis(v) for v in x)
    return False


POSLEDNYAYA_OSHIBKA = {"tekst": ""}
# По этой подписи сервер не пишет снимок /ask в журнал запросов (src/routes/data.py, SNAPSHOT_UA)
PODPIS = "SIMA-pages-snapshot/1.0"


def zabrat(api: str, adres: str, popytok: int = 2) -> bytes | None:
    for i in range(popytok):
        try:
            zapros = Request(api + adres, headers={"User-Agent": PODPIS, "Accept": "application/json"})
            with urlopen(zapros, timeout=45) as otvet:
                telo = otvet.read()
                json.loads(telo)                      # снимок — только настоящий JSON
                return telo
        except HTTPError as e:
            POSLEDNYAYA_OSHIBKA["tekst"] = f"HTTP {e.code}"
            if e.code < 500:
                return None
        except (URLError, TimeoutError, ValueError) as e:
            # на macOS с Python с python.org здесь бывает CERTIFICATE_VERIFY_FAILED:
            # запускать с SSL_CERT_FILE="$(python3 -m certifi)"; на Ubuntu в Actions не нужно
            POSLEDNYAYA_OSHIBKA["tekst"] = f"{type(e).__name__}: {e}"
        time.sleep(2 * (i + 1))
    return None


# ── Прослойка: подмена fetch, выключение управления, плашка демо ────────────────
SHIM = r"""<script>
/* СИМА — демо-копия для GitHub Pages (scripts/dlya_pages.py). Данные — снимок ответов
   сервера, а не живой API: fetch от корня читает api/<ключ>.json. Управление выключено. */
window.SIMA_STATIC = __META__;
(function () {
  var S = window.SIMA_STATIC, realFetch = window.fetch.bind(window), realOpen = window.open.bind(window);
  var MANAGE = new RegExp('^/(' + S.manage.map(function (p) { return p.replace(/[.*+?^${}()|[\]\\\/]/g, '\\$&'); }).join('|') + ')(/|\\?|#|$)');
  function kanon(url) {
    var u = new URL(url, 'http://x'), put = decodeURIComponent(u.pathname), def = S.defaults[put] || {}, par = [];
    u.searchParams.forEach(function (v, k) { if (def[k] !== v) par.push([k, v]); });
    par.sort(function (a, b) { return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0); });
    return put + (par.length ? '?' + par.map(function (p) { return p[0] + '=' + p[1]; }).join('&') : '');
  }
  function fnv(s) {
    var b = new TextEncoder().encode(s), h = 0x811c9dc5;
    for (var i = 0; i < b.length; i++) { h ^= b[i]; h = Math.imul(h, 0x01000193) >>> 0; }
    return ('0000000' + h.toString(16)).slice(-8);
  }
  function json(status, obj) {
    return new Response(JSON.stringify(obj), { status: status, headers: { 'Content-Type': 'application/json' } });
  }
  var NET = 'В демо-версии без сервера это недоступно. Полная версия СИМА — ' + S.source;
  window.SIMA_STATIC.key = function (url) { return 'api/' + fnv(kanon(url)) + '.json'; };
  window.fetch = function (input, init) {
    var url = typeof input === 'string' ? input : (input && input.url) || '';
    if (url.charAt(0) !== '/' || url.charAt(1) === '/') return realFetch(input, init);
    var method = ((init && init.method) || (input && input.method) || 'GET').toUpperCase();
    if (url.indexOf(S.base + '/') === 0) return realFetch(input, init);
    if (url.indexOf('/mapgl-key') === 0) return Promise.resolve(json(200, { available: false, demo: true }));
    // действие (отзыв, ответы, обновление) — плашка и отказ; чтение страниц управления —
    // как недоступная сеть: код табло ловит её и просто не рисует блок (задачи мэра)
    if (method !== 'GET') { toast(); return Promise.resolve(json(403, { error: NET, detail: NET, demo: true })); }
    if (MANAGE.test(url)) return Promise.reject(new TypeError('СИМА: в демо-версии недоступно'));
    return realFetch(S.base + '/' + window.SIMA_STATIC.key(url)).then(function (r) {
      if (r.ok) return r;
      console.info('СИМА демо: нет в снимке —', kanon(url));
      if (url.indexOf('/ask') === 0) return json(200, { error: 'Этого запроса нет в демо-снимке: без сервера отвечают только подсказанные запросы.', available_topics: S.examples, demo: true });
      return json(404, { error: 'Нет в демо-снимке', demo: true });
    });
  };
  function toast() {
    var t = document.getElementById('simaDemoToast');
    if (!t) {
      t = document.createElement('div'); t.id = 'simaDemoToast';
      t.style.cssText = 'position:fixed;left:50%;bottom:24px;transform:translateX(-50%);z-index:10000;background:#1e293b;color:#fff;padding:10px 14px;border-radius:12px;font:13px system-ui,sans-serif;max-width:90vw;box-shadow:0 8px 24px rgba(0,0,0,.25)';
      document.body.appendChild(t);
    }
    t.textContent = NET; t.style.display = 'block';
    clearTimeout(t._h); t._h = setTimeout(function () { t.style.display = 'none'; }, 3500);
  }
  window.open = function (u) {
    if (typeof u === 'string' && u.charAt(0) === '/' && MANAGE.test(u)) { toast(); return null; }
    return realOpen.apply(window, arguments);
  };
  document.addEventListener('click', function (e) {
    var a = e.target.closest && e.target.closest('[data-demo-off]');
    if (a) { e.preventDefault(); e.stopPropagation(); toast(); }
  }, true);
  document.addEventListener('DOMContentLoaded', function () {
    var bar = document.createElement('div');
    bar.style.cssText = 'background:#fef3c7;color:#78350f;font:12px system-ui,sans-serif;padding:6px 12px;text-align:center;border-bottom:1px solid #fde68a';
    bar.textContent = 'Демо-копия СИМА без сервера: данные на ' + S.snapshotLocal + ', обновляются раз в час. Страницы управления недоступны. Полная версия — ' + S.source;
    document.body.insertBefore(bar, document.body.firstChild);
  });
})();
</script>
<style>[data-demo-off]{opacity:.45;cursor:not-allowed}</style>
"""


META_CHARSET = re.compile(r"""<meta\s+charset=["']?utf-8["']?\s*/?>""", re.I)


def sobrat_stranicu(html: str, osnova: str, meta: dict) -> str:
    if len(META_CHARSET.findall(html)) != 1:
        sys.exit('СТОП: в index.html не ровно один <meta charset="UTF-8"> — разметка изменилась, поправьте dlya_pages.py')
    # Прослойка — сразу ПОСЛЕ <meta charset>, а не после <head>: браузер ищет кодировку
    # в начале файла, и вставка перед ней отодвигала её. Без charset в заголовке ответа
    # начало страницы читалось как windows-1251 — кракозябры в строках прослойки
    # (проверка в браузере 2026-11-17). Pages шлёт charset, но полагаться на это не надо.
    shim = SHIM.replace("__META__", json.dumps(meta, ensure_ascii=False))
    html = META_CHARSET.sub(lambda m: m.group(0) + "\n" + shim, html, count=1)
    html = SSYLKA_UPRAVLENIYA.sub(
        'href="#demo-off" data-demo-off="1" aria-disabled="true" title="В демо-версии недоступно"', html)
    # ресурсы и фото — под подпапку репозитория (в атрибутах и в строках скриптов)
    for prefiks in ("/static/", "/ciinsu/photo/"):
        html = re.sub(r"""(['"`=])""" + re.escape(prefiks), lambda m: m.group(1) + osnova + prefiks, html)
    return html


def foto_iz_novostej(dannye) -> set[str]:
    najdeno: set[str] = set()
    def obhod(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k in ("photo", "image", "img") and isinstance(v, str) and v and "/" not in v.strip("/"):
                    najdeno.add(v.strip("/"))
                obhod(v)
        elif isinstance(x, list):
            for v in x:
                obhod(v)
    obhod(dannye)
    return najdeno


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--osnova", required=True, help="подпапка сайта на Pages, например /NSK_OpenData_Bot")
    p.add_argument("--vyhod", default=str(KOREN / "_pages"), help="куда положить копию")
    p.add_argument("--api", default="https://nsk-opendata-bot.up.railway.app", help="откуда снимать данные")
    p.add_argument("--potoki", type=int, default=6, help="параллельных запросов к API")
    a = p.parse_args()
    osnova = "/" + a.osnova.strip("/")
    api = a.api.rstrip("/")
    vyhod = Path(a.vyhod).resolve()
    if vyhod in (STATIC.resolve(), KOREN):
        sys.exit("СТОП: выход не может совпадать с исходниками")
    if vyhod.exists():
        shutil.rmtree(vyhod)
    (vyhod / "api").mkdir(parents=True)

    # ресурсы: картинки, схемы метро, manifest — без страниц управления
    shutil.copytree(STATIC / "img", vyhod / "static" / "img", ignore=shutil.ignore_patterns(".DS_Store"))
    for jpg in STATIC.glob("*.jpg"):
        shutil.copy2(jpg, vyhod / "static" / jpg.name)
    manifest = json.loads((STATIC / "manifest.json").read_text(encoding="utf-8"))
    manifest["start_url"] = osnova + "/"
    for ikonka in manifest.get("icons", []):
        ikonka["src"] = osnova + ikonka["src"] if ikonka["src"].startswith("/") else ikonka["src"]
    (vyhod / "static" / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # снимок
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    telo = zabrat(api, "/api/city-config")
    if telo is None:
        sys.exit(f"СТОП: {api}/api/city-config не отвечает ({POSLEDNYAYA_OSHIBKA['tekst']}) — "
                 "снимок не снят, публикация не меняется")
    konfig = json.loads(telo)
    adresa = spisok_adresov(html, konfig)
    poluceno, propusk, ne_hranim, bajt = 0, [], [], 0
    start = time.monotonic()
    # ответы по 0,5–2 с, адресов ~400: последовательно — больше 10 минут, потоками — 2–3
    with ThreadPoolExecutor(max_workers=a.potoki) as pul:
        otvety = list(pul.map(lambda x: zabrat(api, x), adresa))
    for adres, telo in zip(adresa, otvety):
        if telo is None:
            propusk.append(adres)
            continue
        if dannye_2gis(json.loads(telo)):
            ne_hranim.append(adres)                   # прослойка ответит «нет в демо-снимке»
            continue
        (vyhod / fajl_snimka(adres)).write_bytes(telo)
        poluceno += 1
        bajt += len(telo)
        if adres == "/ciinsu/news":
            for imya in sorted(foto_iz_novostej(json.loads(telo))):
                try:
                    foto = urlopen(Request(f"{api}/ciinsu/photo/{quote(imya)}",
                                           headers={"User-Agent": PODPIS}), timeout=60).read()
                except Exception:
                    continue                          # нет фото — новость покажется без картинки
                put = vyhod / "ciinsu" / "photo" / imya
                put.parent.mkdir(parents=True, exist_ok=True)
                put.write_bytes(foto)

    sejchas = datetime.now(timezone.utc)
    primery = [q for q in ("отключения ЖКХ сейчас", "качество воздуха в городе", "плановые отключения на неделю",
                           "рейтинг качества жкх по районам", "риски для жизни в городе")]
    meta = {
        "base": osnova,
        "source": api.replace("https://", ""),
        "snapshot": sejchas.isoformat(timespec="seconds"),
        "snapshotLocal": (sejchas + timedelta(hours=7)).strftime("%d.%m.%Y %H:%M") + " по Новосибирску",
        "manage": list(UPRAVLENIE),
        "defaults": UMOLCHANIYA,
        "examples": primery,
    }
    (vyhod / "index.html").write_text(sobrat_stranicu(html, osnova, meta), encoding="utf-8")
    (vyhod / "404.html").write_text(
        f'<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="0; url={osnova}/">'
        f'<a href="{osnova}/">СИМА — демо</a>', encoding="utf-8")
    (vyhod / ".nojekyll").write_text("", encoding="utf-8")
    (vyhod / "api" / "_snapshot.json").write_text(json.dumps({
        "snapshot": meta["snapshot"], "source": api, "requested": len(adresa), "saved": poluceno,
        "skipped": propusk, "not_stored_2gis": ne_hranim,
        "keys": {kanon(x): fajl_snimka(x) for x in adresa if x not in propusk and x not in ne_hranim},
    }, ensure_ascii=False, indent=1), encoding="utf-8")

    # ── самопроверка ──────────────────────────────────────────────────────────
    oshibki = []
    stranica = (vyhod / "index.html").read_text(encoding="utf-8")
    if stranica.count("window.SIMA_STATIC = ") != 1:
        oshibki.append("прослойка вставлена не ровно один раз")
    bajty = stranica.encode("utf-8")
    m_charset = re.search(rb"<meta\s+charset", bajty, re.I)
    if not m_charset or m_charset.start() > 900 or m_charset.start() > bajty.find(b"window.SIMA_STATIC"):
        oshibki.append("<meta charset> не в начале файла или после прослойки — страница прочитается не в UTF-8")
    for _, znachenie in ATRIBUT.findall(stranica):
        if not znachenie.startswith(osnova + "/"):
            oshibki.append(f"адрес от корня без основы: {znachenie}")
    for prefiks in ("/static/", "/ciinsu/photo/"):
        for m in re.finditer(r"""(['"`=])""" + re.escape(prefiks), stranica):
            oshibki.append(f"строка без основы: …{stranica[m.start():m.start() + 40]}")
    for adres in VAZHNYE:
        f = vyhod / fajl_snimka(adres)
        if not f.exists() or f.stat().st_size < 2:
            oshibki.append(f"нет важного снимка: {adres}")
    if oshibki:
        print("СТОП, самопроверка:\n  " + "\n  ".join(oshibki[:20]))
        sys.exit(1)
    print(f"{vyhod.name}/ — копия лендинга под {osnova}/: снимков {poluceno} из {len(adresa)} "
          f"({bajt / 1024 / 1024:.1f} МБ) за {time.monotonic() - start:.0f} с, пропущено {len(propusk)}, "
          f"не хранится по правилам 2ГИС {len(ne_hranim)}; адресов без основы нет.")
    for x in propusk[:15]:
        print("  пропущено:", x)
    for x in ne_hranim[:15]:
        print("  данные 2ГИС, не хранится:", x)


if __name__ == "__main__":
    main()
