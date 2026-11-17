"""
══════════════════════════════════════════════════════════════════════════════
  ТЕСТЫ ОБНОВЛЕНИЯ УСТАРЕВШИХ ДАННЫХ БЕЗ ОЖИДАНИЯ (src/stale_refresh.py)
  BEKLOG № 12 · разбор docs/OSM-OBNOVLENIE-RAZBOR.md
══════════════════════════════════════════════════════════════════════════════
"""

import inspect
import threading
import time

import pytest

import src.stale_refresh as sr


@pytest.fixture(autouse=True)
def _chisto():
    sr.reset_state()
    yield
    sr.reset_state()


def test_свежие_данные_не_обновляются():
    vyzovy = []
    assert sr.refresh_if_stale("t", lambda: False, lambda: True, lambda: vyzovy.append(1)) == "fresh"
    assert vyzovy == []


def test_устаревшие_при_кэше_отвечают_сразу_и_обновляют_в_фоне():
    """О-1: запрос не ждёт закачку, если отвечать есть чем."""
    pusk, gotovo = threading.Event(), threading.Event()

    def medlenno():
        pusk.set()
        gotovo.wait(5)
        return 10

    t0 = time.monotonic()
    assert sr.refresh_if_stale("t", lambda: True, lambda: True, medlenno) == "background"
    assert time.monotonic() - t0 < 0.5
    assert pusk.wait(2)                                   # обновление действительно идёт
    # О-3: второе обращение, пока идёт первое, новую закачку не запускает
    assert sr.refresh_if_stale("t", lambda: True, lambda: True, medlenno) == "running"
    gotovo.set()


def test_без_данных_качаем_в_запросе():
    """О-2: отвечать нечем — обновление синхронно, как раньше."""
    potok = []
    assert sr.refresh_if_stale("t", lambda: True, lambda: False,
                               lambda: potok.append(threading.current_thread()) or 5) == "sync"
    assert potok == [threading.current_thread()]


@pytest.mark.parametrize("neudacha", [lambda: 0, lambda: None, lambda: 1 / 0])
def test_после_неудачи_пауза(neudacha, monkeypatch):
    """О-4: недоступный Overpass не держит каждый следующий запрос."""
    assert sr.refresh_if_stale("t", lambda: True, lambda: False, neudacha) == "sync"
    vyzovy = []
    assert sr.refresh_if_stale("t", lambda: True, lambda: False, lambda: vyzovy.append(1) or 1) == "cooldown"
    assert vyzovy == []
    monkeypatch.setattr(sr, "COOLDOWN_SECONDS", 0)       # пауза вышла — снова пробуем
    assert sr.refresh_if_stale("t", lambda: True, lambda: False, lambda: vyzovy.append(1) or 1) == "sync"
    assert vyzovy == [1]


def test_ask_по_камерам_не_ждёт_overpass(monkeypatch):
    """Сквозная проверка: устаревшие камеры при кэше — ответ без ожидания трёхсекундной закачки."""
    import src.cameras_cache as cc
    import src.cameras_fetcher as cf
    from fastapi.testclient import TestClient
    from src.api import app

    pusk = threading.Event()

    def medlenno():
        pusk.set()
        time.sleep(3)
        return []

    monkeypatch.setattr(cc, "is_cameras_stale", lambda: True)
    monkeypatch.setattr(cc, "get_cameras_meta", lambda: {"total_rows": 3, "last_updated": "2026-08-20"})
    monkeypatch.setattr(cc, "count_cameras", lambda **kw: 3)
    monkeypatch.setattr(cc, "query_cameras", lambda **kw: [])
    monkeypatch.setattr(cf, "fetch_cameras", medlenno)
    t0 = time.monotonic()
    r = TestClient(app).get("/ask", params={"q": "камеры видеофиксации"})
    proshlo = time.monotonic() - t0
    assert r.status_code == 200 and r.json().get("topic") == "cameras"
    assert pusk.wait(2) and proshlo < 2.5, proshlo


def test_в_ask_не_осталось_синхронной_закачки():
    """Три места «устарело → скачать → записать» идут через refresh_if_stale."""
    import src.routes.data as data_routes
    kod = inspect.getsource(data_routes.get_ask) + inspect.getsource(data_routes._handle_osm_topic)
    for staroe in ("if is_cameras_stale():", "if is_medical_stale():", "if is_osm_topic_stale(topic):"):
        assert staroe not in kod, staroe
    assert kod.count("refresh_if_stale(") == 3
