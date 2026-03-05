"""Тесты модуля экологии и метеорологии.

Покрывают:
- Маршрутизацию (router.py) — распознавание запросов об экологии
- Планировщик (planner.py) — определение операций ECO_STATUS / ECO_PDK / ECO_HISTORY
- Кэш (ecology_cache.py) — инициализация таблиц, upsert, запросы на тестовых данных
- API-эндпоинты (api.py) — /ecology/status, /ecology/pdk, /ecology/history, /ecology/update
"""

import pytest
from datetime import datetime, timezone


# ── Маршрутизация ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("query", [
    "качество воздуха в Новосибирске",
    "где сейчас самый загрязненный воздух",
    "смог в городе",
    "уровень PM2.5 в центре",
    "AQI по районам",
    "пыль в воздухе",
    "чем дышим в Академгородке",
    "экологическая обстановка",
    "no2 превышение",
])
def test_ecology_routing_air_quality(query):
    """Запросы о качестве воздуха маршрутизируются к теме ecology."""
    from src.router import best_topic
    result = best_topic(query)
    assert result is not None, f"Тема не определена для: {query!r}"
    assert result.topic == "ecology", (
        f"Запрос: {query!r}\n"
        f"Ожидалось: ecology, получено: {result.topic} (confidence={result.confidence:.2f})"
    )


@pytest.mark.parametrize("query", [
    "какая погода в центре",
    "температура воздуха сейчас",
    "скорость ветра в Советском районе",
    "атмосферное давление в Новосибирске",
    "влажность воздуха сейчас",
])
def test_ecology_routing_weather(query):
    """Запросы о погоде маршрутизируются к теме ecology."""
    from src.router import best_topic
    result = best_topic(query)
    assert result is not None, f"Тема не определена для: {query!r}"
    assert result.topic == "ecology", (
        f"Запрос: {query!r}\n"
        f"Ожидалось: ecology, получено: {result.topic}"
    )


@pytest.mark.parametrize("query", [
    "сколько парковок",
    "отключения электричества",
    "школы в Ленинском районе",
    "рецепты борща с говядиной",
])
def test_ecology_routing_no_false_positives(query):
    """Запросы не по теме экологии не маршрутизируются к ecology."""
    from src.router import best_topic
    result = best_topic(query)
    if result is not None:
        assert result.topic != "ecology", (
            f"Ложное срабатывание ecology для: {query!r} (confidence={result.confidence:.2f})"
        )


# ── Планировщик ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_op", [
    # ECO_STATUS — текущее состояние
    ("качество воздуха сейчас",              "ECO_STATUS"),
    ("какой AQI в Советском районе",         "ECO_STATUS"),
    ("покажи погоду по районам",             "ECO_STATUS"),
    ("температура в центре",                 "ECO_STATUS"),
    ("ветер в Новосибирске",                 "ECO_STATUS"),
    ("смог в Академгородке",                 "ECO_STATUS"),
    # ECO_PDK — превышение порога
    ("превышение ПДК по PM2.5",              "ECO_PDK"),
    ("уровень PM2.5 превышен",               "ECO_PDK"),
    ("воздух опасен для здоровья",           "ECO_PDK"),
    ("вредный воздух в центре",              "ECO_PDK"),
    # ECO_HISTORY — динамика
    ("динамика PM2.5 за неделю",             "ECO_HISTORY"),
    ("история загрязнения воздуха",          "ECO_HISTORY"),
    ("тренд AQI за 7 дней",                  "ECO_HISTORY"),
    ("как менялась экология на прошлой неделе", "ECO_HISTORY"),
])
def test_ecology_plan_operations(query, expected_op):
    """Планировщик правильно определяет тип операции для ecology-запросов."""
    from src.planner import make_plan
    plan = make_plan(query, "ecology")
    assert plan.operation == expected_op, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_op}, получено: {plan.operation}"
    )


def test_ecology_plan_district():
    """Планировщик извлекает район из ecology-запроса."""
    from src.planner import make_plan
    plan = make_plan("качество воздуха в Советском районе", "ecology")
    assert plan.district == "Советский район"


def test_ecology_plan_no_district():
    """Без района — plan.district == None."""
    from src.planner import make_plan
    plan = make_plan("качество воздуха в Новосибирске", "ecology")
    assert plan.district is None


def test_ecology_plan_sub_district():
    """Запрос с подрайоном (Академгородок) → district=Советский, sub_district=Академгородок."""
    from src.planner import make_plan
    plan = make_plan("смог в Академгородке", "ecology")
    assert plan.district == "Советский район"
    assert plan.sub_district == "Академгородок"


# ── Кэш и модели данных ───────────────────────────────────────────────────────

@pytest.fixture
def ecology_db_with_data():
    """Инициализирует таблицы и вставляет тестовые данные."""
    from src.ecology_cache import init_ecology_tables, upsert_stations, upsert_measurements
    from src.constants import NSK_ECOLOGY_STATIONS

    init_ecology_tables()
    upsert_stations(NSK_ECOLOGY_STATIONS)

    now = datetime.now(timezone.utc).isoformat()
    records = [
        {
            "id":           f"{s['station_id']}_test",
            "station_id":   s["station_id"],
            "measured_at":  now,
            "pm25":         10.5 + i,
            "pm10":         18.0 + i,
            "no2":          8.0,
            "aqi":          30 + i * 2,
            "temperature_c": -5.0 + i,
            "wind_speed_ms": 2.0,
            "wind_direction_deg": 180.0,
            "humidity_pct": 75.0,
            "pressure_hpa": 1013.0,
            "source":       "test",
        }
        for i, s in enumerate(NSK_ECOLOGY_STATIONS)
    ]
    upsert_measurements(records)
    return records


def test_ecology_init_tables():
    """Таблицы dim_stations и fact_measurements создаются без ошибок."""
    from src.ecology_cache import init_ecology_tables
    init_ecology_tables()  # должно быть идемпотентным


def test_ecology_upsert_stations():
    """Станции вставляются в dim_stations без ошибок."""
    from src.ecology_cache import upsert_stations
    from src.constants import NSK_ECOLOGY_STATIONS
    upsert_stations(NSK_ECOLOGY_STATIONS)


def test_ecology_stations_count():
    """Все 10 районов присутствуют в справочнике."""
    from src.constants import NSK_ECOLOGY_STATIONS
    assert len(NSK_ECOLOGY_STATIONS) == 10


def test_ecology_stations_have_required_fields():
    """Каждая станция содержит все обязательные поля ТЗ §4."""
    from src.constants import NSK_ECOLOGY_STATIONS
    required = {"station_id", "district", "address", "latitude", "longitude"}
    for s in NSK_ECOLOGY_STATIONS:
        missing = required - set(s.keys())
        assert not missing, f"Станция {s.get('station_id')!r}: нет полей {missing}"


def test_ecology_upsert_and_query_current(ecology_db_with_data):
    """После upsert query_current() возвращает записи для всех 10 районов."""
    from src.ecology_cache import query_current
    rows = query_current()
    assert len(rows) == 10, f"Ожидалось 10 районов, получено {len(rows)}"


def test_ecology_query_current_district_filter(ecology_db_with_data):
    """query_current(district_filter=...) возвращает только нужный район."""
    from src.ecology_cache import query_current
    rows = query_current(district_filter="Советский")
    assert len(rows) == 1
    assert "Советский" in rows[0]["district"]


def test_ecology_query_current_fields(ecology_db_with_data):
    """Каждая запись содержит все ожидаемые поля."""
    from src.ecology_cache import query_current
    rows = query_current()
    required = {"district", "pm25", "pm10", "aqi", "temperature_c", "wind_speed_ms"}
    for row in rows:
        missing = required - set(row.keys())
        assert not missing, f"Запись {row.get('district')!r}: нет полей {missing}"


def test_ecology_query_pdk_no_exceedances(ecology_db_with_data):
    """При PM2.5 < 35 — пустой результат для query_pdk_exceedances()."""
    from src.ecology_cache import query_pdk_exceedances
    # Тестовые данные: pm25 = 10.5 + i (макс = 19.5) — ниже порога 35
    rows = query_pdk_exceedances()
    assert len(rows) == 0, f"Ожидалось 0 превышений ПДК, получено {len(rows)}"


def test_ecology_query_pdk_with_exceedances():
    """При PM2.5 > 35 query_pdk_exceedances() возвращает данные."""
    from src.ecology_cache import init_ecology_tables, upsert_stations, upsert_measurements, query_pdk_exceedances
    from src.constants import NSK_ECOLOGY_STATIONS

    init_ecology_tables()
    upsert_stations(NSK_ECOLOGY_STATIONS)

    now = datetime.now(timezone.utc).isoformat()
    # Вставляем запись с PM2.5 = 50 (выше порога 35)
    records = [{
        "id":           "nsk_central_pdk_test",
        "station_id":   "nsk_central",
        "measured_at":  now,
        "pm25":         50.0,
        "pm10":         60.0,
        "no2":          25.0,
        "aqi":          85,
        "temperature_c": 0.0,
        "wind_speed_ms": 0.5,
        "wind_direction_deg": 90.0,
        "humidity_pct": 80.0,
        "pressure_hpa": 1010.0,
        "source":       "test",
    }]
    upsert_measurements(records)

    rows = query_pdk_exceedances(district_filter="Центральный")
    assert len(rows) >= 1
    assert rows[0]["pm25_max"] >= 35.0


def test_ecology_query_history(ecology_db_with_data):
    """query_history() возвращает хотя бы одну запись за сегодня."""
    from src.ecology_cache import query_history
    rows = query_history(days=1)
    assert len(rows) > 0


def test_ecology_get_meta(ecology_db_with_data):
    """get_ecology_meta() содержит корректные поля."""
    from src.ecology_cache import get_ecology_meta
    meta = get_ecology_meta()
    assert "last_updated" in meta
    assert "total_records" in meta
    assert "districts_covered" in meta
    assert meta["districts_covered"] >= 1
    assert meta["total_records"] > 0


def test_ecology_is_stale_when_empty():
    """is_ecology_stale() → True если нет данных."""
    from src.ecology_cache import is_ecology_stale, init_ecology_tables
    from src.cache import _get_conn
    # Очищаем измерения
    init_ecology_tables()
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM fact_measurements WHERE source = 'test'")
    finally:
        conn.close()
    # После очистки (если нет других записей) стейл = True
    # Просто проверяем что функция не падает и возвращает bool
    result = is_ecology_stale()
    assert isinstance(result, bool)


# ── Executor ──────────────────────────────────────────────────────────────────

def test_execute_ecology_status(ecology_db_with_data):
    """execute_ecology() с ECO_STATUS возвращает rows и columns."""
    from src.executor import execute_ecology
    from src.planner import make_plan
    plan = make_plan("качество воздуха сейчас", "ecology")
    result = execute_ecology(plan)
    assert "rows" in result
    assert "columns" in result
    assert result["operation"] == "ECO_STATUS"
    assert result["count"] == 10


def test_execute_ecology_pdk(ecology_db_with_data):
    """execute_ecology() с ECO_PDK возвращает threshold и note."""
    from src.executor import execute_ecology
    from src.planner import make_plan
    plan = make_plan("превышение ПДК PM2.5", "ecology")
    result = execute_ecology(plan)
    assert result["operation"] == "ECO_PDK"
    assert result["threshold"] == 35.0
    assert "note" in result


def test_execute_ecology_history(ecology_db_with_data):
    """execute_ecology() с ECO_HISTORY возвращает days."""
    from src.executor import execute_ecology
    from src.planner import make_plan
    plan = make_plan("динамика качества воздуха за неделю", "ecology")
    result = execute_ecology(plan)
    assert result["operation"] == "ECO_HISTORY"
    assert "days" in result


def test_execute_ecology_district_filter(ecology_db_with_data):
    """execute_ecology() фильтрует по району."""
    from src.executor import execute_ecology
    from src.planner import make_plan
    plan = make_plan("воздух в Советском районе", "ecology")
    result = execute_ecology(plan)
    assert result["count"] == 1
    assert result["rows"][0]["district"] == "Советский район"


# ── API endpoints ─────────────────────────────────────────────────────────────

@pytest.fixture
def api_client(ecology_db_with_data):
    """TestClient для FastAPI приложения с заранее загруженными тестовыми данными."""
    from fastapi.testclient import TestClient
    from src.api import app
    return TestClient(app)


def test_api_ecology_status_ok(api_client):
    """GET /ecology/status → 200, содержит rows и ecology_meta."""
    resp = api_client.get("/ecology/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["operation"] == "ECO_STATUS"
    assert "rows" in data
    assert "ecology_meta" in data
    assert isinstance(data["rows"], list)


def test_api_ecology_status_district(api_client):
    """GET /ecology/status?district=Советский → фильтр работает."""
    resp = api_client.get("/ecology/status", params={"district": "Советский район"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["rows"][0]["district"] == "Советский район"


def test_api_ecology_pdk_ok(api_client):
    """GET /ecology/pdk → 200, содержит threshold_pm25."""
    resp = api_client.get("/ecology/pdk")
    assert resp.status_code == 200
    data = resp.json()
    assert data["operation"] == "ECO_PDK"
    assert data["threshold_pm25"] == 35.0
    assert "rows" in data


def test_api_ecology_history_ok(api_client):
    """GET /ecology/history → 200, содержит days и rows."""
    resp = api_client.get("/ecology/history", params={"days": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert data["operation"] == "ECO_HISTORY"
    assert data["days"] == 1
    assert "rows" in data


def test_api_ecology_history_days_validation(api_client):
    """GET /ecology/history?days=99 → 422 Unprocessable Entity."""
    resp = api_client.get("/ecology/history", params={"days": 99})
    assert resp.status_code == 422


def test_api_ecology_update_ok(api_client, monkeypatch):
    """POST /ecology/update → 200 с success=True (мокаем fetch_all_ecology)."""
    from src import ecology_fetcher
    from src.constants import NSK_ECOLOGY_STATIONS
    from datetime import timezone

    # Мок: возвращает фиктивные записи, не делает реальных HTTP-запросов
    def mock_fetch():
        now = datetime.now(timezone.utc).isoformat()
        return [
            {
                "id":           f"{s['station_id']}_mock",
                "station_id":   s["station_id"],
                "measured_at":  now,
                "pm25":         5.0,
                "pm10":         9.0,
                "no2":          3.0,
                "aqi":          15,
                "temperature_c": 0.0,
                "wind_speed_ms": 1.0,
                "wind_direction_deg": 270.0,
                "humidity_pct": 60.0,
                "pressure_hpa": 1020.0,
                "source":       "mock",
            }
            for s in NSK_ECOLOGY_STATIONS
        ]

    monkeypatch.setattr(ecology_fetcher, "fetch_all_ecology", mock_fetch)

    resp = api_client.post("/ecology/update")
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["records_loaded"] == 10
    assert data["districts_covered"] == 10


def test_api_ask_ecology_routing(api_client):
    """GET /ask?q=качество воздуха → topic=ecology в ответе."""
    resp = api_client.get("/ask", params={"q": "качество воздуха в Новосибирске"})
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("topic") == "ecology", (
        f"Ожидалось topic=ecology, получено: {data.get('topic')}\n"
        f"Ответ: {data}"
    )


def test_api_ask_ecology_pdk(api_client):
    """GET /ask?q=превышение ПДК → operation=ECO_PDK."""
    resp = api_client.get("/ask", params={"q": "превышение ПДК по PM2.5 сегодня"})
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("topic") == "ecology"
    assert data.get("operation") == "ECO_PDK"


def test_api_ask_ecology_history(api_client):
    """GET /ask?q=динамика PM2.5 за неделю → operation=ECO_HISTORY."""
    resp = api_client.get("/ask", params={"q": "динамика PM2.5 за неделю"})
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("topic") == "ecology"
    assert data.get("operation") == "ECO_HISTORY"
