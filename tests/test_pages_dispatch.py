"""Запуск обновления демо-копии с сервера (src/pages_dispatch.py) — PAGES-DEMO-RAZBOR, Р-16."""

import asyncio
import time

import src.pages_dispatch as pd


def test_запуск_workflow_по_api_github():
    vyzov = {}

    class Otvet:
        status_code = 204

    def post(url, **kw):
        vyzov.update(url=url, **kw)
        return Otvet()

    assert pd.dispatch_once("ТОКЕН", "o/r", "pages.yml", "main", post=post) == 204
    assert vyzov["url"] == "https://api.github.com/repos/o/r/actions/workflows/pages.yml/dispatches"
    assert vyzov["json"] == {"ref": "main"}
    assert vyzov["headers"]["Authorization"] == "Bearer ТОКЕН"


def test_без_токена_цикл_сразу_выходит(monkeypatch):
    monkeypatch.delenv("GITHUB_PAGES_TOKEN", raising=False)
    t0 = time.monotonic()
    asyncio.run(pd.pages_dispatch_loop(initial_delay=60))
    assert time.monotonic() - t0 < 1
