"""Запуск обновления демо-копии на GitHub Pages с сервера (workflow_dispatch).

Расписание GitHub (`schedule` в .github/workflows/pages.yml) ненадёжно: 17.11.2026 за шесть
часов после подключения не случилось ни одного планового запуска — только запуски от слияний
(workflow активен, ветка по умолчанию main, сбоев GitHub Actions не было). Документация GitHub
предупреждает: плановые запуски при нагрузке задерживаются и могут выпадать.

Сервер работает круглосуточно и раз в час просит GitHub запустить workflow. Нужен токен GitHub
с правом «Actions: Read and write» на этот репозиторий — переменная окружения GITHUB_PAGES_TOKEN
(на Railway). Без токена цикл молчит, остаётся расписание GitHub.
Разбор — docs/PAGES-DEMO-RAZBOR.md, Р-16.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Callable

log = logging.getLogger(__name__)

API = "https://api.github.com"


def dispatch_once(token: str, repo: str, workflow: str, ref: str, post: Callable | None = None) -> int:
    """Попросить GitHub запустить workflow. 204 — запуск поставлен в очередь."""
    if post is None:
        import requests
        post = requests.post
    r = post(
        f"{API}/repos/{repo}/actions/workflows/{workflow}/dispatches",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "SIMA-pages-dispatch",
        },
        json={"ref": ref},
        timeout=30,
    )
    return r.status_code


async def pages_dispatch_loop(initial_delay: float = 300.0) -> None:
    """Раз в GITHUB_PAGES_INTERVAL_MIN минут (по умолчанию 60) — запуск workflow Pages."""
    token = os.environ.get("GITHUB_PAGES_TOKEN", "").strip()
    if not token:
        log.info("pages_dispatch: GITHUB_PAGES_TOKEN не задан — демо-копия обновляется "
                 "только по расписанию GitHub")
        return
    repo = os.environ.get("GITHUB_PAGES_REPO", "Barbashin1970/NSK_OpenData_Bot")
    workflow = os.environ.get("GITHUB_PAGES_WORKFLOW", "pages.yml")
    ref = os.environ.get("GITHUB_PAGES_REF", "main")
    interval = max(15.0, float(os.environ.get("GITHUB_PAGES_INTERVAL_MIN", "60"))) * 60
    await asyncio.sleep(initial_delay)
    while True:
        try:
            code = await asyncio.to_thread(dispatch_once, token, repo, workflow, ref)
            if code == 204:
                log.info("pages_dispatch: обновление демо-копии запущено (%s, %s)", workflow, ref)
            else:
                log.warning("pages_dispatch: GitHub ответил %s — проверьте токен и его права", code)
        except Exception as exc:                       # сбой сети не должен ронять сервер
            log.warning("pages_dispatch: запуск не удался: %s", exc)
        await asyncio.sleep(interval)
