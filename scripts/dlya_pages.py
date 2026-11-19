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
import os
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
# Функции страницы, которые в демо выключены. Кнопки с ними прячутся, вызов — плашка.
# - игра «Пятнашки» и пользовательские отчёты (МТС, аварийные службы) — просьба 2026-11-17;
# - плашки «Центр ИИ НГУ» и «Новости ЦИИ НГУ» и панель ЦИИ (в том числе по запросу «что такое
#   ЦИИ») — просьба 2026-11-18: в демо для мэрии это вне поля внимания мэра.
OTKLYUCHIT = ("toggleGamePanel", "_showCustomReport", "toggleCiiPanel", "loadNewsPublic", "loadCiiSection")
# Адреса, которые в демо намеренно пусты: прослойка отвечает «нет» сразу, без запроса в сеть
# (без списка отчётов страница не рисует их карточки; данные ЦИИ в демо не снимаются)
PUSTYE = ("custom-reports", "ciinsu")


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


# ── Районы в запросах — так же, как их склоняет и вырезает страница ───────────────
def predlozhnyj(name: str) -> str:
    """_distPrep: «Советский район» → «Советском районе»."""
    name = re.sub(r"ский\s+(район|округ)", lambda m: f"ском {m.group(1)}е", name, flags=re.I)
    return re.sub(r"ный\s+(район|округ)", lambda m: f"ном {m.group(1)}е", name, flags=re.I)


def okonchanie_rajona(label: str, val: str) -> str:
    """_distQuerySuffix: «Советский» → « в Советском районе»."""
    prep = re.sub(r"ный$", "ном", re.sub(r"ский$", "ском", label))
    return f" в {prep} {'округе' if (val or '').lower().endswith('округ') else 'районе'}"


def bez_rajona(q: str, rajony: list[dict]) -> str:
    """_stripDistrictFromQuery: убрать из запроса район и подрайон."""
    s = q
    for d in rajony:
        if not d.get("val"):
            continue
        stem = re.sub(r"ая$", "", re.sub(r"ое$", "", re.sub(r"[иы]й$", "", d["label"])))
        if len(stem) < 3:
            continue
        s = re.sub(r"\s*(в\s+)?" + stem + r"[а-яёa-z]*\s*(район[еа]?|р-не?|округ[еа]?)?", " ", s, flags=re.I)
    s = re.sub(r"\s*(в\s+)?(?:академгородк|шлюз|кольцов)[а-яёa-z]*\s*", " ", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip()


def varianty_po_rajonam(q: str, rajony: list[dict]) -> list[str]:
    """Запросы кнопок «Район:» под таблицей (renderTable): «весь город» и каждый район."""
    baza = bez_rajona(q, rajony)
    return [baza + okonchanie_rajona(d["label"], d["val"]) if d.get("val") else baza
            for d in rajony if not d.get("desc") or not d.get("val")]


# ── Список адресов снимка — из самого index.html, чтобы не разойтись со страницей ──
def podstanovki(konfig: dict, temy: list[str]) -> dict[str, list[str]]:
    """Значения ${…} в шаблонах запросов страницы — как их подставляет сама страница."""
    vse = [d for d in (konfig.get("districts") or []) if d.get("val")]
    info = [d for d in vse if not d.get("desc")]                    # _infoDists в renderInfo
    p = {
        "dShort": [d["label"] for d in vse],
        "esc(dist)": [d["val"].replace(" район", "") for d in vse],   # карточки районов экологии
        "t.name": list(temy),                                         # карточки наборов: «покажи …»
        "_cityCfg.metro_name": [konfig["metro_name"]] if konfig.get("metro_name") else [],
        "_cityCfg.airport_name": [konfig["airport_name"]] if konfig.get("airport_name") else [],
    }
    for k in ("_d1.prep", "_d2.prep", "_d3.prep"):                  # _rndDist → _distPrep
        p[k] = [predlozhnyj(d["val"]) for d in info]
    p["_tr"] = [d["label"] for d in info]                           # _rndPair: два разных района
    return p


def razvernut_shablony(html: str, konfig: dict, temy: list[str], dop: dict | None = None) -> tuple[set[str], set[str]]:
    """Шаблоны запросов с ${…}: строки в askNow/setQuery и `…` вида запроса. Возвращает
    развёрнутые запросы и подстановки, которых скрипт не знает (видно в журнале сборки).
    dop — значения, известные только из ответов (номера линий метро: ${id})."""
    p = {**podstanovki(konfig, temy), **(dop or {})}
    shablony = {m[1] for m in re.findall(r"""(?:askNow|setQuery)\((['"])([^'"<>\n]*?\$\{[^}]+\}[^'"<>\n]*?)\1\)""", html)}
    shablony |= {s for s in re.findall(r"`([^`<>\n]{3,100})`", html) if "${" in s}
    gotovye: set[str] = set()
    neznakomye: set[str] = set()
    for s in shablony:
        mesta = re.findall(r"\$\{([^}]+)\}", s)
        if not re.search(r"[а-яё]{3}", re.sub(r"\$\{[^}]+\}", "", s), re.I):
            continue                                   # не запрос: одни подстановки
        if set(mesta) == {"_tr1", "_tr2"}:
            gotovye |= {s.replace("${_tr1}", a).replace("${_tr2}", b) for a in p["_tr"] for b in p["_tr"] if a != b}
            continue
        if len(mesta) != 1 or mesta[0] not in p:
            neznakomye |= set(mesta) - set(p)
            continue
        gotovye |= {s.replace("${" + mesta[0] + "}", v) for v in p[mesta[0]]}
    return gotovye, neznakomye


def zaprosy_iz_stranicy(html: str, konfig: dict, temy: list[str] = ()) -> list[str]:
    """Готовые запросы для /ask: литералы, шаблоны с подстановками, чипы ролей."""
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
    gotovye |= razvernut_shablony(html, konfig, list(temy))[0]
    return sorted(q for q in gotovye if q and len(q) < 120)


def spisok_adresov(html: str, konfig: dict, temy: list[str] = ()) -> list[str]:
    gorod = konfig.get("city_id") or "novosibirsk"
    rajony = [r for r in (konfig.get("districts") or []) if r.get("val")]
    adresa = [
        "/api/city-config", "/api/available-cities", "/topics", "/api/vocabulary", "/api/version", "/api/version/check",
        "/ecology/status", "/ecology/risks", "/traffic-index", "/transit/districts", "/power/forecast",
        "/ecology/snow-outlook",                                  # снегопад по районам (docs/SNEGOPAD-RAZBOR.md)
        # /custom-reports не снимается: карточки отчётов (МТС, аварийные службы) в демо не нужны
        # (просьба заказчика 2026-11-17) — без списка страница их не рисует
        # /ciinsu/news и разделы ЦИИ не снимаются: плашки ЦИИ в демо выключены (2026-11-18)
        f"/budget/{gorod}", "/budget-compare", f"/emissions/{gorod}",
        "/ecology/aqi-exceedances?days=30&aqi_threshold=40",
        "/ask?q=" + quote("отключения ЖКХ сейчас", safe=""),       # табло: без параметров страницы
    ]
    for d in PERIODY:
        adresa += [f"/power/history?days={d}&group_by=day", f"/power/history?days={d}&group_by=district",
                   f"/power/efficiency?days={d}", f"/ecology/history?days={d}",
                   f"/ecology/rating-districts?days={d}"]
    for r in rajony:
        v = quote(r["val"], safe="")
        adresa.append("/power/forecast?district=" + quote(FORECAST_RAJON.get(r["val"], r["val"]), safe=""))
        adresa.append("/ecology/snow-outlook?district=" + quote(FORECAST_RAJON.get(r["val"], r["val"]), safe=""))
        adresa.append(f"/ecology/aqi-exceedances?days=30&aqi_threshold=40&district={v}")
        for d in PERIODY:
            adresa += [f"/power/history?days={d}&group_by=day&district={v}", f"/ecology/history?days={d}&district={v}"]
    for q in zaprosy_iz_stranicy(html, konfig, temy):
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


def zabrat(api: str, adres: str, popytok: int = 2, tajmaut: int = 45) -> bytes | None:
    for i in range(popytok):
        try:
            zapros = Request(api + adres, headers={"User-Agent": PODPIS, "Accept": "application/json"})
            with urlopen(zapros, timeout=tajmaut) as otvet:
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


def zaprosy_strok_zhkh(telo: bytes | None) -> list[str]:
    """Вопросы строк аварий в блоке ЖКХ панели служащего (Р-21).

    Строки кликабельны: askNow(«отключения <услуга> в <район>») — renderOfficialDashboard, pwrRows:
    активные отключения с домами, без строк «all». Без снимка этих вопросов прослойка прятала
    строки, и блок выглядел пустым (18.11). Шаблон повторяет страницу: первое слово услуги
    строчными, район как в ответе.
    """
    try:
        d = json.loads(telo) if telo else {}
    except ValueError:
        return []
    out: list[str] = []
    for r in (d.get("rows") or []) if isinstance(d, dict) else []:
        if not isinstance(r, dict) or r.get("group_type") != "active":
            continue
        try:
            doma = float(r.get("houses") or 0)
        except (TypeError, ValueError):
            continue
        rajon = r.get("district") or ""
        if doma > 0 and rajon and rajon != "all":
            q = f"отключения {(r.get('utility') or '').split(' ')[0].lower()} в {rajon}"
            if q not in out:
                out.append(q)
    return out


def dozhdatsya_servera(api: str, sha: str | None = None, limit_s: float = 480, pauza: float = 15,
                       poluchit=None, grace_s: float = 90) -> str:
    """Ждёт, пока сервер отвечает, а если он сообщает коммит — тот же, что у сборки (Р-19).

    Пуш в main одновременно перевыкатывает Railway и запускает эту сборку: снимок, снятый в
    момент перезапуска, терял важные адреса (запуск №11, 18.11 — /ecology/status), а снятый до
    перезапуска — показывал старую версию сервера. Коммит сервер берёт из RAILWAY_GIT_COMMIT_SHA,
    сборка — из GITHUB_SHA. Сервер коммит не сообщает (старая версия или не Railway) — один раз
    ждём grace_s: вдруг выкатка ещё идёт, — и проверяем снова. По пределу времени идём дальше:
    решит самопроверка, прошлая публикация при отказе остаётся.
    """
    poshchada = grace_s
    poluchit = poluchit or (lambda: zabrat(api, "/api/version", popytok=1, tajmaut=20))
    t0 = time.monotonic()
    while True:
        telo = poluchit()
        try:
            d = json.loads(telo) if telo else None
        except ValueError:
            d = None
        if isinstance(d, dict):
            kommit = d.get("commit")
            if not sha:
                return "готов"
            if not kommit:
                if poshchada > 0:                     # один раз переждать возможную выкатку
                    time.sleep(poshchada)
                    poshchada = 0
                    continue
                return "готов (коммит сервер не сообщает)"
            if kommit.startswith(sha) or sha.startswith(kommit):
                return "готов"
        if time.monotonic() - t0 >= limit_s:
            return "не дождались"
        time.sleep(pauza)


def dobrat_vazhnye(otvety: dict, poluchit, popytok: int = 3, pauza: float = 20) -> list[str]:
    """Важные адреса, не снятые в общем проходе, переспросить после паузы (Р-19).

    Перезапуск Railway длится 30–60 с, а общий проход переспрашивает адрес через 2–4 с.
    Возвращает то, что так и не снялось.
    """
    for _ in range(popytok):
        nedostayot = [x for x in VAZHNYE if otvety.get(x) is None]
        if not nedostayot:
            return []
        time.sleep(pauza)
        for x in nedostayot:
            otvety[x] = poluchit(x)
    return [x for x in VAZHNYE if otvety.get(x) is None]


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
  // выключенные функции страницы (S.off): их кнопки не видны сразу, ещё до отрисовки
  if ((S.off || []).length && document.head) {
    var st = document.createElement('style');
    st.textContent = S.off.map(function (f) { return '[onclick^="' + f + '"]'; }).join(',') + '{display:none!important}';
    document.head.appendChild(st);
  }
  // Ключи снимка (_snapshot.json, один раз): подсказки при промахе и скрытие кнопок без ответа
  var KLYUCHI = Promise.resolve().then(function () { return realFetch(S.base + '/api/_snapshot.json'); })
    .then(function (r) { return r.json(); }).then(function (j) { return j.keys || {}; }).catch(function () { return null; });
  function spisok() {
    return KLYUCHI.then(function (k) {
      return Object.keys(k || {}).filter(function (x) { return x.indexOf('/ask?q=') === 0 && x.indexOf('&') < 0; })
        .map(function (x) { return x.slice(7); });
    });
  }
  var STOP = {'покажи': 1, 'какие': 1, 'какой': 1, 'сколько': 1, 'где': 1, 'все': 1, 'для': 1, 'как': 1, 'что': 1,
              'это': 1, 'район': 1, 'районе': 1, 'город': 1, 'города': 1, 'городе': 1};
  function slova(s) {
    return (s.toLowerCase().replace(/ё/g, 'е').match(/[а-яa-z0-9]+/g) || []).filter(function (w) { return w.length > 2 && !STOP[w]; });
  }
  function rodnye(a, b) {          // «школ» ~ «школы», «аптек» ~ «аптеки», но «парки» ≁ «парковки»
    var n = 0;
    while (n < a.length && n < b.length && a.charAt(n) === b.charAt(n)) n++;
    return n >= Math.min(5, a.length, b.length);
  }
  function pohozhie(q, vse) {
    var qs = slova(q);
    if (!qs.length) return [];
    return vse.map(function (t) {
      var ts = slova(t), hit = 0;
      qs.forEach(function (a) { if (ts.some(function (b) { return rodnye(a, b); })) hit++; });
      return [hit * 10 - Math.max(0, ts.length - hit), t];
    }).filter(function (p) { return p[0] >= 10; })
      .sort(function (x, y) { return y[0] - x[0] || x[1].length - y[1].length; })
      .slice(0, 5).map(function (p) { return p[1]; });
  }
  function ekr(s) { return String(s).replace(/[&<>"']/g, function (c) { return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]; }); }
  function ssylka(t) { return '<a href="#" data-sima-q="' + ekr(t) + '" style="color:#1d4ed8;text-decoration:underline">' + ekr(t) + '</a>'; }
  window.SIMA_STATIC.key = function (url) { return 'api/' + fnv(kanon(url)) + '.json'; };
  window.fetch = function (input, init) {
    var url = typeof input === 'string' ? input : (input && input.url) || '';
    if (url.charAt(0) !== '/' || url.charAt(1) === '/') return realFetch(input, init);
    var method = ((init && init.method) || (input && input.method) || 'GET').toUpperCase();
    if (url.indexOf(S.base + '/') === 0) return realFetch(input, init);
    if (url.indexOf('/mapgl-key') === 0) return Promise.resolve(json(200, { available: false, demo: true }));
    if ((S.empty || []).some(function (p) { return new RegExp('^/' + p + '(/|\\?|$)').test(url); }))
      return Promise.resolve(json(404, { error: 'В демо-версии не показывается', demo: true }));
    // действие (отзыв, ответы, обновление) — плашка и отказ; чтение страниц управления —
    // как недоступная сеть: код табло ловит её и просто не рисует блок (задачи мэра)
    if (method !== 'GET') { toast(); return Promise.resolve(json(403, { error: NET, detail: NET, demo: true })); }
    if (MANAGE.test(url)) return Promise.reject(new TypeError('СИМА: в демо-версии недоступно'));
    return realFetch(S.base + '/' + window.SIMA_STATIC.key(url)).then(function (r) {
      if (r.ok) return r;
      console.info('СИМА демо: нет в снимке —', kanon(url));
      if (url.indexOf('/ask') === 0) {
        var q = new URL(url, 'http://x').searchParams.get('q') || '';
        return spisok().then(function (vse) {
          var p = pohozhie(q, vse);
          return json(200, { error: 'Этого запроса нет в демо-снимке: без сервера копия отвечает только на сохранённые запросы (их ' + vse.length + ').',
                             available_topics: (p.length ? p : S.examples).map(ssylka), demo: true });
        });
      }
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
    if (a) { e.preventDefault(); e.stopPropagation(); toast(); return; }
    var s = e.target.closest && e.target.closest('[data-sima-q]');      // подсказка при промахе
    if (s && typeof window.askNow === 'function') { e.preventDefault(); e.stopPropagation(); window.askNow(s.getAttribute('data-sima-q')); }
  }, true);
  // Кнопка, чьего запроса нет в снимке (тема не отвечает на сервере, вторая страница…), прячется:
  // лучше меньше кнопок, чем «нет в демо-снимке» по клику. Кнопки появляются по ходу отрисовки —
  // отсюда наблюдатель. Основной сайт не меняется.
  function zaprosyKnopki(code) {
    var out = [];
    try { new Function('askNow', 'setQuery', code)(function (q) { out.push(q); }, function (q) { out.push(q); }); } catch (e) {}
    return out;
  }
  function spryatat(root, k) {
    var els = [].slice.call(root.querySelectorAll ? root.querySelectorAll('[onclick]') : []);
    if (root.getAttribute && root.getAttribute('onclick')) els.unshift(root);
    els.forEach(function (el) {
      var code = el.getAttribute('onclick') || '';
      if (!/askNow\(|setQuery\(/.test(code)) return;
      var qs = zaprosyKnopki(code);
      if (qs.length && qs.every(function (q) { return !k[kanon('/ask?q=' + encodeURIComponent(q))]; })) el.style.display = 'none';
    });
  }
  KLYUCHI.then(function (k) {
    if (!k) return;
    function nachat() {
      spryatat(document.body, k);
      new MutationObserver(function (ms) {
        ms.forEach(function (m) { [].forEach.call(m.addedNodes, function (n) { if (n.nodeType === 1) spryatat(n, k); }); });
      }).observe(document.body, { childList: true, subtree: true });
    }
    if (document.body) nachat(); else document.addEventListener('DOMContentLoaded', nachat);
  });
  document.addEventListener('DOMContentLoaded', function () {
    // вызов выключенной функции другим путём (запрос «пятнашки» → игра) — только плашка
    (S.off || []).forEach(function (f) { if (typeof window[f] === 'function') window[f] = function () { toast(); }; });
    var bar = document.createElement('div');
    bar.style.cssText = 'background:#fef3c7;color:#78350f;font:12px system-ui,sans-serif;padding:6px 12px;text-align:center;border-bottom:1px solid #fde68a';
    bar.textContent = 'Демо-копия РАГРАФ-ПРОГНОЗ без сервера: данные на ' + S.snapshotLocal + ', обновляются раз в час. Страницы управления недоступны. Полная версия — ' + S.source;
    document.body.insertBefore(bar, document.body.firstChild);
  });
})();
</script>
<style>[data-demo-off]{opacity:.45;cursor:not-allowed}</style>
"""


META_CHARSET = re.compile(r"""<meta\s+charset=["']?utf-8["']?\s*/?>""", re.I)

# ── Демо в стиле РАГРАФ (просьба руководителя работы 2026-11-18) ───────────────────
# Вместо «мишки» — знак Σ, как в шапке лендинга РАГРАФ (синий квадрат, белая Σ, Inter Bold).
# Значок вкладки — белая Σ в синем круге (Р-23, 19.11): в цвет знака в шапке, а в строке браузера
# не путается с вкладкой РАГРАФ, у которой золотая Σ на тёмном квадрате. Имя — «РАГРАФ-ПРОГНОЗ», ниже —
# «Система интеллектуального муниципального анализа». Шапка темнее — slate-900 → blue-950 →
# blue-800, как тёмный первый экран РАГРАФ. Меняется только копия для Pages; основной сайт —
# с прежним видом. Каждая замена обязана найтись ровно один раз, иначе сборка останавливается:
# демо со смесью старого и нового вида хуже прошлой публикации.
IMYA_DEMO = "РАГРАФ-ПРОГНОЗ"
PODZAG_DEMO = "Система интеллектуального муниципального анализа"
ZNACHOK_SVG = ("<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>"
               "<circle cx='32' cy='32' r='32' fill='#2563EB'/>"
               "<text x='50%' y='50%' text-anchor='middle' dominant-baseline='central' "
               "font-family='system-ui,-apple-system,Segoe UI,Roboto,sans-serif' font-weight='700' "
               "font-size='40' fill='#FFFFFF'>Σ</text></svg>")
TYOMNAYA_SHAPKA = "bg-gradient-to-br from-slate-900 via-blue-950 to-blue-800"
# Ссылка на лендинг РАГРАФ — в подвале демо (решение руководителя работы 2026-11-18: только в демо,
# основной сайт СИМА не меняется). Девиз — заголовок лендинга РАГРАФ.
SSYLKA_RAGRAF = "https://ragraf.up.railway.app/"
# Второй продукт в подвале — Платформа АРХИ (просьба руководителя работы 2026-11-19, Р-22); подпись —
# по заголовку её публичной страницы: «Платформа АРХИ — контроль строительных регламентов».
SSYLKA_ARHI = "https://barbashin1970.github.io/ARCHI-NSU-Public/"


def _znak(klass: str, extra: str = "") -> str:
    """Знак Σ в синем квадрате — как в шапке лендинга РАГРАФ."""
    return (f'<span class="{klass} flex flex-shrink-0 items-center justify-center bg-blue-600 text-white '
            f'shadow-lg ring-1 ring-white/25" style="font-weight:700;line-height:1;'
            f'font-family:\'Inter\',system-ui,sans-serif"{extra}>Σ</span>')


PEREODET = [
    ("<title>СИМА — Открытые данные Новосибирска</title>",
     f"<title>{IMYA_DEMO} — {PODZAG_DEMO}</title>"),
    ('  <link rel="icon" type="image/svg+xml" href="/static/img/favicon.svg">\n'
     '  <link rel="icon" type="image/png" href="/static/img/sigma-cat-sm.png">\n'
     '  <link rel="apple-touch-icon" href="/static/img/sigma-cat.png">\n',
     f'  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,{quote(ZNACHOK_SVG, safe="")}">\n'),
    ('  <link rel="preload" as="image" href="/static/img/sigma-cat-sm.png">\n', ""),
    ('<header class="bg-gradient-to-br from-blue-900 via-blue-700 to-indigo-700 text-white shadow-xl">',
     f'<header class="{TYOMNAYA_SHAPKA} text-white shadow-xl">'),
    ('        <img src="/static/img/sigma-cat-sm.png"\n'
     '             class="h-14 sm:h-20 w-auto object-contain flex-shrink-0 cursor-pointer active:scale-95 transition-transform"\n'
     '             alt="СИМА"\n'
     '             title="Что умеет СИМА?"\n'
     '             onclick="setQuery(\'что ты умеешь\')"\n'
     '             style="filter:drop-shadow(0 2px 8px rgba(0,0,0,.5))">\n'
     '        <div class="min-w-0">\n'
     '          <h1 class="text-2xl sm:text-3xl font-bold leading-tight tracking-tight">СИМА</h1>\n'
     '          <p class="text-blue-300 text-xs sm:text-sm mt-0.5 sm:mt-1">Открытые данные — только спроси</p>\n',
     "        " + _znak("h-12 w-12 sm:h-16 sm:w-16 rounded-xl text-3xl sm:text-4xl cursor-pointer active:scale-95 "
                     "transition-transform",
                     ' role="button" tabindex="0" title="Что умеет система?" aria-label="Что умеет система"'
                     ' onclick="setQuery(\'что ты умеешь\')"') + "\n"
     '        <div class="min-w-0">\n'
     f'          <h1 class="text-2xl sm:text-3xl font-bold leading-tight tracking-tight">{IMYA_DEMO}</h1>\n'
     f'          <p class="text-blue-200 text-xs sm:text-sm mt-0.5 sm:mt-1">{PODZAG_DEMO}</p>\n'),
    ('<div class="bg-gradient-to-br from-blue-900 via-blue-700 to-indigo-700 px-5 py-4 text-white">',
     f'<div class="{TYOMNAYA_SHAPKA} px-5 py-4 text-white">'),
    ('<img src="/static/img/sigma-cat-sm.png" class="h-16 w-auto flex-shrink-0 object-contain drop-shadow-md" alt="Сима">',
     _znak("h-12 w-12 rounded-xl text-3xl", ' aria-hidden="true"')),
    ('<p class="font-semibold text-base leading-snug">Привет! Я — Сима</p>',
     f'<p class="font-semibold text-base leading-snug">{IMYA_DEMO}</p>'),
    ("СИМА · ЦИИ НГУ · rule-based, без AI в runtime · моделирование и прогнозирование",
     f"{IMYA_DEMO} · {PODZAG_DEMO} · ЦИИ НГУ · правила, без AI в runtime · моделирование и прогнозирование"),
    ('<p class="font-medium text-slate-500">СИМА — система интеллектуального муниципального анализа <span id="footerVer"',
     f'<p class="font-medium text-slate-500">{IMYA_DEMO} — система интеллектуального муниципального анализа <span id="footerVer"'),
    (' class="inline-block bg-slate-100 border border-slate-200 rounded px-1.5 py-0.5 text-[10px] text-slate-400 '
     'font-mono ml-1"></span></p>\n',
     ' class="inline-block bg-slate-100 border border-slate-200 rounded px-1.5 py-0.5 text-[10px] text-slate-400 '
     'font-mono ml-1"></span></p>\n'
     f'    <p class="text-slate-500">Линейка <a href="{SSYLKA_RAGRAF}" target="_blank" rel="noopener" '
     'class="font-bold text-slate-600 hover:text-blue-600 transition-colors">РАГРАФ ↗</a> — '
     'искусственный интеллект, который можно проверить</p>\n'
     f'    <p class="text-slate-500"><a href="{SSYLKA_ARHI}" target="_blank" rel="noopener" '
     'class="font-bold text-slate-600 hover:text-blue-600 transition-colors">Платформа АРХИ ↗</a> — '
     'контроль строительных регламентов</p>\n'),
]


def pereodet(html: str) -> str:
    """Копия страницы для Pages — в стиле РАГРАФ: знак Σ, имя «РАГРАФ-ПРОГНОЗ», тёмная шапка."""
    for staroe, novoe in PEREODET:
        n = html.count(staroe)
        if n != 1:
            sys.exit(f"СТОП: переодевание демо — фрагмент найден {n} раз(а), разметка index.html изменилась: "
                     f"{staroe[:80]!r}")
        html = html.replace(staroe, novoe)
    return html


def sobrat_stranicu(html: str, osnova: str, meta: dict) -> str:
    if len(META_CHARSET.findall(html)) != 1:
        sys.exit('СТОП: в index.html не ровно один <meta charset="UTF-8"> — разметка изменилась, поправьте dlya_pages.py')
    # Прослойка — сразу ПОСЛЕ <meta charset>, а не после <head>: браузер ищет кодировку
    # в начале файла, и вставка перед ней отодвигала её. Без charset в заголовке ответа
    # начало страницы читалось как windows-1251 — кракозябры в строках прослойки
    # (проверка в браузере 2026-11-17). Pages шлёт charset, но полагаться на это не надо.
    shim = SHIM.replace("__META__", json.dumps(meta, ensure_ascii=False))
    html = pereodet(html)                                  # вид РАГРАФ — до вставки прослойки
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

    # снимок — когда Railway закончил выкатку того же коммита (Р-19)
    print(f"сервер: {dozhdatsya_servera(api, os.environ.get('GITHUB_SHA') or None)}")
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    telo = zabrat(api, "/api/city-config")
    if telo is None:
        sys.exit(f"СТОП: {api}/api/city-config не отвечает ({POSLEDNYAYA_OSHIBKA['tekst']}) — "
                 "снимок не снят, публикация не меняется")
    konfig = json.loads(telo)
    rajony_vse = konfig.get("districts") or []
    telo = zabrat(api, "/topics")
    temy_json = json.loads(telo) if telo else {}
    temy = [t["name"] for t in (temy_json.get("topics", []) if isinstance(temy_json, dict) else temy_json)
            if isinstance(t, dict) and t.get("name")]
    start = time.monotonic()
    otvety: dict[str, bytes | None] = {}

    def snyat(spisok: list[str], popytok: int, tajmaut: int) -> None:
        # ответы по 0,5–2 с, адресов сотни: последовательно — десятки минут, потоками — минуты
        uzhe, novye = {kanon(y) for y in otvety}, []
        for x in spisok:
            if kanon(x) not in uzhe:
                uzhe.add(kanon(x))
                novye.append(x)
        with ThreadPoolExecutor(max_workers=a.potoki) as pul:
            otvety.update(zip(novye, pul.map(lambda x: zabrat(api, x, popytok, tajmaut), novye)))

    def ask(q: str) -> str:
        return "/ask?q=" + quote(q, safe="")

    # Шаг 0 — проба: по карточке «покажи …» на тему. Тема, не ответившая за 30 с (сервер
    # перекачивает OSM — BEKLOG № 12), выпадает из производных запросов: иначе зависшие
    # запросы держали бы сборку и раз в час нагружали Railway попытками Overpass.
    snyat([ask(f"покажи {t}") for t in temy], 1, 30)
    # образец темы: начала слов названия — «детск…сад», «спорт…площа» (и «спортплощадки»),
    # чтобы «спортивные площадки» не цепляли «спортивные организации»
    zavisli = [r"\w*\s*".join(w[:max(3, min(5, len(w) - 1))] for w in t.lower().split())
               for t in temy if otvety.get(ask(f"покажи {t}")) is None]

    def zhivoj(adres: str) -> bool:
        q = dict(parse_qsl(urlsplit(adres).query)).get("q", "").lower() if adres.startswith("/ask") else ""
        return not any(re.search(k, q) for k in zavisli)

    # Шаг 1 — всё, что страница показывает и предлагает: адреса API, готовые запросы, шаблоны
    snyat([x for x in spisok_adresov(html, konfig, temy) if zhivoj(x) or x in VAZHNYE], 2, 45)
    ne_snyaty = dobrat_vazhnye(otvety, lambda x: zabrat(api, x, 2, 45))
    if ne_snyaty:
        print(f"важные адреса не снялись и после повторов: {', '.join(ne_snyaty)}")
    # Шаг 2 — кнопки «Район:» под каждой таблицей из снимка (renderTable)
    tablicy = []
    for adres, telo in list(otvety.items()):
        if not telo or not adres.startswith("/ask"):
            continue
        d = json.loads(telo)
        if isinstance(d, dict) and isinstance(d.get("rows"), list) and isinstance(d.get("columns"), list) \
                and d.get("operation") in ("FILTER", "TOP_N", "GROUP", "COUNT"):
            tablicy.append(dict(parse_qsl(urlsplit(adres).query)).get("q", ""))
    vidannye = {kanon(x) for x in otvety}
    shag2 = []
    for q in tablicy:
        for v in varianty_po_rajonam(q, rajony_vse):
            if v and kanon(ask(v)) not in vidannye and zhivoj(ask(v)):
                vidannye.add(kanon(ask(v)))
                shag2.append(ask(v))
    # кнопки «Линия N» в ответе про метро: askNow('станции метро линия ${id}'), номера — из ответа
    linii = sorted({str(k) for x, telo in otvety.items() if telo and x.startswith("/ask")
                    for d in [json.loads(telo)] if isinstance(d, dict) and d.get("topic") == "metro"
                    and isinstance(d.get("lines"), dict) for k in d["lines"]})
    for v in sorted(razvernut_shablony(html, konfig, temy, {"id": linii})[0]):
        if kanon(ask(v)) not in vidannye and zhivoj(ask(v)):
            vidannye.add(kanon(ask(v)))
            shag2.append(ask(v))
    # строки аварий в блоке ЖКХ панели служащего — их вопросы из ответа «отключения ЖКХ сейчас» (Р-21)
    for v in zaprosy_strok_zhkh(otvety.get(ask("отключения ЖКХ сейчас"))):
        if kanon(ask(v)) not in vidannye:
            vidannye.add(kanon(ask(v)))
            shag2.append(ask(v))
    snyat(shag2, 1, 30)

    adresa = list(otvety)
    poluceno, propusk, ne_hranim, bajt = 0, [], [], 0
    for adres, telo in otvety.items():
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
        "off": list(OTKLYUCHIT),
        "empty": list(PUSTYE),
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
    if "sigma-cat" in stranica or f">{IMYA_DEMO}</h1>" not in stranica:
        oshibki.append("вид РАГРАФ не применён: остался «мишка» или нет заголовка «РАГРАФ-ПРОГНОЗ»")
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
    print(f"  кнопки «Район:» под таблицами (шаг 2): {len(shag2)} адресов")
    if zavisli:
        print("  темы не ответили за 30 с — их запросы не снимаются, кнопки к ним прослойка прячет:",
              ", ".join(t for t in temy if otvety.get(ask(f"покажи {t}")) is None))
    neznakomye = razvernut_shablony(html, konfig, temy)[1]
    if neznakomye:
        print("  шаблоны с незнакомыми подстановками не развёрнуты:", ", ".join(sorted(neznakomye)[:12]))


if __name__ == "__main__":
    main()
