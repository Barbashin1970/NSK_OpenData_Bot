"""
══════════════════════════════════════════════════════════════════════════════
  ТЕСТЫ ДЕМО-КОПИИ ДЛЯ GITHUB PAGES (scripts/dlya_pages.py)
  Разбор: docs/PAGES-DEMO-RAZBOR.md
══════════════════════════════════════════════════════════════════════════════

Каждая проверка — по дефекту, найденному при проверке копии в браузере 2026-11-17.
"""

import importlib.util
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

KOREN = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("dlya_pages", KOREN / "scripts" / "dlya_pages.py")
dp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dp)

META = {"base": "/NSK_OpenData_Bot", "source": "example", "snapshot": "", "snapshotLocal": "",
        "manage": list(dp.UPRAVLENIE), "defaults": dp.UMOLCHANIYA, "examples": []}


def test_charset_в_начале_страницы_и_до_прослойки():
    """Р-8: прослойка перед <meta charset> отодвигала его — начало страницы читалось
    как windows-1251, в плашке и ответах прослойки были кракозябры."""
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    bajty = dp.sobrat_stranicu(html, META["base"], META).encode("utf-8")
    poz = bajty.lower().find(b"<meta charset")
    assert 0 <= poz < 900
    assert poz < bajty.find(b"window.SIMA_STATIC")


def test_параметры_по_умолчанию_не_меняют_ключ():
    """Р-9: ask() шлёт /ask?q=…, листалка — с offset=0&page_size=20; ответ сервера один."""
    q = "/ask?q=%D0%B0%D0%BF%D1%82%D0%B5%D0%BA%D0%B8"
    assert dp.fajl_snimka(q) == dp.fajl_snimka(q + "&offset=0&page_size=20")
    assert dp.fajl_snimka(q) != dp.fajl_snimka(q + "&offset=20&page_size=20")


def test_примеры_в_двойных_кавычках_попадают_в_снимок():
    """Проверка живого сайта: набор примеров ролей записан как {q: "…"} — 25 из 43
    примеров, в том числе видимые на табло чипы, отвечали «нет в демо-снимке»."""
    from urllib.parse import quote
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    adresa = {dp.kanon(a) for a in dp.spisok_adresov(html, {"city_id": "novosibirsk", "districts": []})}
    for q in ("топ-5 аптек", "культурные организации", "больницы с приёмным покоем",
              "аптеки в Центральном районе", "пробки сейчас"):
        assert dp.kanon("/ask?q=" + quote(q, safe="")) in adresa, q


RAJONY = [{"val": "", "label": "Весь город", "desc": "Сводные данные по всем районам"}] + [
    {"val": f"{x} район", "label": x, "desc": ""} for x in (
        "Дзержинский", "Железнодорожный", "Заельцовский", "Калининский", "Кировский",
        "Ленинский", "Октябрьский", "Первомайский", "Советский", "Центральный")] + [
    {"val": "Кольцово", "label": "Кольцово", "desc": ""},
    {"val": "Академгородок", "label": "Академгородок", "desc": "Советский р-н"},
    {"val": "Шлюз", "label": "Шлюз", "desc": "Советский р-н"}]
KONFIG = {"city_id": "novosibirsk", "districts": RAJONY,
          "metro_name": "Новосибирский метрополитен", "airport_name": "Аэропорт Толмачёво"}
TEMY = ["Школы", "Детские сады", "Аптеки", "Остановки транспорта", "Парковки"]


def _node_or_skip():
    node = shutil.which("node")
    if not node:
        pytest.skip("node не установлен")
    return node


def test_районы_склоняются_и_вырезаются_как_на_странице(tmp_path):
    """Шаг 2 снимает кнопки «Район:» — их текст обязан совпасть с тем, что пошлёт страница."""
    node = _node_or_skip()
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    funkcii = "\n".join(re.search(rf"function {f}\(.*?\n\}}\n", html, re.S).group(0)
                        for f in ("_distPrep", "_stripDistrictFromQuery", "_distQuerySuffix"))
    zaprosy = ["аптеки в Ленинском районе", "покажи Школы в Центральном районе", "школы Советский",
               "детские сады в Академгородке", "топ-5 аптек", "качество воздуха в Кольцово",
               "библиотеки для детей в Железнодорожном р-не", "отключения в Октябрьском районе сейчас"]
    stend = (f"let NSK_DISTRICTS = {json.dumps(RAJONY, ensure_ascii=False)};\n{funkcii}\n"
             f"console.log(JSON.stringify({{prep: NSK_DISTRICTS.map(d => _distPrep(d.val)),"
             f" suf: NSK_DISTRICTS.map(d => _distQuerySuffix(d.label, d.val)),"
             f" strip: {json.dumps(zaprosy, ensure_ascii=False)}.map(_stripDistrictFromQuery)}}));")
    f = tmp_path / "rajony.js"
    f.write_text(stend, encoding="utf-8")
    js = json.loads(subprocess.run([node, str(f)], capture_output=True, text=True, check=True).stdout)
    assert js["prep"] == [dp.predlozhnyj(d["val"]) for d in RAJONY]
    assert js["suf"] == [dp.okonchanie_rajona(d["label"], d["val"]) for d in RAJONY]
    assert js["strip"] == [dp.bez_rajona(q, RAJONY) for q in zaprosy]


def test_шаблоны_страницы_разворачиваются():
    """Живой сайт 17.11: карточки «покажи …», метро, аэропорт, районы из блока «Сервисы» и
    экологии отвечали «нет в демо-снимке» — их запросы строятся на лету."""
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    zaprosy, _ = dp.razvernut_shablony(html, KONFIG, TEMY)
    for q in ("покажи Школы", "покажи Детские сады", "метро Новосибирский метрополитен",
              "аэропорт Аэропорт Толмачёво", "аптеки в Ленинском районе", "парковки в Кольцово",
              "как проехать из Советский в Центральный", "качество воздуха в Советский"):
        assert q in zaprosy, q
    assert "покажи Школы в Ленинском районе" in dp.varianty_po_rajonam("покажи Школы", RAJONY)
    assert "аптеки" in dp.varianty_po_rajonam("аптеки в Октябрьском районе", RAJONY)   # «Весь город»


def test_промах_подсказывает_похожие_запросы(tmp_path):
    """Вместо пяти постоянных примеров — сохранённые запросы с общими словами, по ним можно нажать."""
    node = _node_or_skip()
    meta = dict(META, examples=["отключения ЖКХ сейчас"])
    shim = dp.SHIM.replace("__META__", json.dumps(meta, ensure_ascii=False))
    js = shim.split("<script>", 1)[1].split("</script>", 1)[0]
    klyuchi = {f"/ask?q={q}": "api/x.json" for q in ("школы в Ленинском районе", "аптеки в Центральном районе",
                                                      "парковки в Ленинском районе", "покажи Школы")}
    stend = ("globalThis.window = globalThis; window.open = () => {};\n"
             "globalThis.document = { addEventListener() {} };\n"
             f"const KLYUCHI = {json.dumps(klyuchi, ensure_ascii=False)};\n"
             "window.fetch = (u) => Promise.resolve(u.endsWith('/api/_snapshot.json')\n"
             "  ? new Response(JSON.stringify({keys: KLYUCHI}), {status: 200}) : new Response('', {status: 404}));\n"
             + js +
             "\nwindow.fetch('/ask?q=' + encodeURIComponent('школы ленинский')).then(r => r.json())"
             ".then(d => console.log('ИТОГ ' + JSON.stringify(d)));")
    f = tmp_path / "promah.js"
    f.write_text(stend, encoding="utf-8")
    vyvod = subprocess.run([node, str(f)], capture_output=True, text=True, check=True).stdout
    d = json.loads(vyvod.split("ИТОГ ", 1)[1])
    assert "нет в демо-снимке" in d["error"]
    assert d["available_topics"][0].startswith('<a href="#" data-sima-q="школы в Ленинском районе"')
    assert not any("аптеки" in t for t in d["available_topics"])


def test_игра_и_отчёты_в_демо_выключены(tmp_path):
    """Просьба заказчика 17.11: «Пятнашки» и отчёты (МТС, аварийные службы — давали 404)
    в демо не показываются; на основном сайте всё остаётся."""
    assert {"toggleGamePanel", "_showCustomReport"} <= set(dp.OTKLYUCHIT)
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    assert not any(a.startswith("/custom-reports") for a in dp.spisok_adresov(html, KONFIG))
    node = _node_or_skip()
    shim = dp.SHIM.replace("__META__", json.dumps(dict(META, off=list(dp.OTKLYUCHIT)), ensure_ascii=False))
    js = shim.split("<script>", 1)[1].split("</script>", 1)[0]
    stend = ("globalThis.window = globalThis; window.open = () => {}; window.fetch = () => Promise.reject();\n"
             "const stili = []; globalThis.document = { addEventListener() {}, head: { appendChild: e => stili.push(e.textContent) },\n"
             "  createElement: () => ({}) };\n" + js + "\nconsole.log(JSON.stringify(stili));")
    f = tmp_path / "off.js"
    f.write_text(stend, encoding="utf-8")
    css = json.loads(subprocess.run([node, str(f)], capture_output=True, text=True, check=True).stdout)
    assert '[onclick^="toggleGamePanel"]' in css[0] and '[onclick^="_showCustomReport"]' in css[0]
    # список отчётов — «нет» сразу, без запроса в сеть (иначе 404 в журнале браузера)
    shim = dp.SHIM.replace("__META__", json.dumps(dict(META, empty=list(dp.PUSTYE)), ensure_ascii=False))
    js = shim.split("<script>", 1)[1].split("</script>", 1)[0]
    stend = ("globalThis.window = globalThis; window.open = () => {}; let set = 0;\n"
             "window.fetch = () => { set++; return Promise.reject(); };\n"
             "globalThis.document = { addEventListener() {} };\n" + js +
             "\nwindow.fetch('/custom-reports').then(r => console.log(JSON.stringify([r.status, set])));")
    f.write_text(stend, encoding="utf-8")
    status, zaprosov = json.loads(subprocess.run([node, str(f)], capture_output=True, text=True, check=True).stdout)
    assert status == 404 and zaprosov == 1        # единственный запрос — _snapshot.json при загрузке


def test_данные_2гис_не_хранятся():
    """Правила 2ГИС: маршруты и справочник — только проброс, без хранения."""
    assert dp.dannye_2gis({"rows": [{"source": "2GIS Public Transport Routing API (real-time, данные не сохраняются)"}]})
    assert not dp.dannye_2gis({"source": "https://opendata.novo-sibirsk.ru · остановки",
                               "notice": "Для точного маршрута воспользуйтесь 2ГИС",
                               "hint": "https://2gis.ru/novosibirsk/routeSearch"})


def test_ключ_снимка_одинаков_в_python_и_в_браузере(tmp_path):
    """Р-2: файл снимка ищет прослойка в браузере — ключи обязаны совпадать байт в байт."""
    node = shutil.which("node")
    if not node:
        pytest.skip("node не установлен")
    adresa = [
        "/api/city-config",
        "/ask?q=%D0%BA%D0%B0%D1%87%D0%B5%D1%81%D1%82%D0%B2%D0%BE%20%D0%B2%D0%BE%D0%B7%D0%B4%D1%83%D1%85%D0%B0",
        "/ask?page_size=20&q=%D0%B0%D0%BF%D1%82%D0%B5%D0%BA%D0%B8&offset=0",
        "/ask?q=%D0%B0%D0%BF%D1%82%D0%B5%D0%BA%D0%B8&offset=20&page_size=20",
        "/power/forecast?district=%D0%A1%D0%BE%D0%B2%D0%B5%D1%82%D1%81%D0%BA%D0%B8%D0%B9",
        "/power/history?group_by=day&days=30&district=%D0%9B%D0%B5%D0%BD%D0%B8%D0%BD%D1%81%D0%BA%D0%B8%D0%B9%20%D1%80%D0%B0%D0%B9%D0%BE%D0%BD",
        "/ciinsu?section=team",
    ]
    shim = dp.SHIM.replace("__META__", json.dumps(META, ensure_ascii=False))
    js = shim.split("<script>", 1)[1].split("</script>", 1)[0]
    stend = ("globalThis.window = globalThis; window.fetch = () => {}; window.open = () => {};\n"
             "globalThis.document = { addEventListener() {} };\n" + js +
             f"\nconsole.log(JSON.stringify({json.dumps(adresa)}.map(u => window.SIMA_STATIC.key(u))));")
    f = tmp_path / "klyuchi.js"
    f.write_text(stend, encoding="utf-8")
    vyvod = subprocess.run([node, str(f)], capture_output=True, text=True, check=True).stdout
    assert json.loads(vyvod) == [dp.fajl_snimka(a) for a in adresa]


def test_снимок_не_пишется_в_журнал_запросов(monkeypatch):
    """Р-10: раз в час ~190 адресов /ask — без этого ~4 500 служебных строк в сутки
    в журнале запросов и его статистике."""
    import inspect
    from fastapi.testclient import TestClient
    from src.api import app
    import src.routes.data as data_routes

    # все записи в get_ask идут через одну привязку _log — прямой вызов её обходит
    assert "_log_query(" not in inspect.getsource(data_routes.get_ask)
    zapisi = []
    monkeypatch.setattr(data_routes, "_log_query", lambda **kw: zapisi.append(kw))
    klient = TestClient(app)
    zapros = {"q": "абракадабра проверка снимка"}       # ветка UNKNOWN: без данных и сети
    klient.get("/ask", params=zapros, headers={"User-Agent": dp.PODPIS})
    assert zapisi == []
    klient.get("/ask", params=zapros)
    assert len(zapisi) == 1 and zapisi[0]["operation"] == "UNKNOWN"
