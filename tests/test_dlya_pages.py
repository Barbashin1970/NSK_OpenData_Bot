"""
══════════════════════════════════════════════════════════════════════════════
  ТЕСТЫ ДЕМО-КОПИИ ДЛЯ GITHUB PAGES (scripts/dlya_pages.py)
  Разбор: docs/PAGES-DEMO-RAZBOR.md
══════════════════════════════════════════════════════════════════════════════

Каждая проверка — по дефекту, найденному при проверке копии в браузере 2026-11-17.
"""

import importlib.util
import json
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
