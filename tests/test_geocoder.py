"""Тесты модуля геокодирования (src/geocoder.py).

Покрывают:
- _address_key()    — детерминированное хеширование
- extract_address() — извлечение адреса из строк всех датасетов
- geocode()         — кеш / API / graceful degradation (нет ключа, 403, таймаут)
- geocode_rows()    — обогащение списка строк координатами
- geocode_stats()   — статистика кеша
"""

import hashlib
from unittest.mock import MagicMock, patch

import pytest


# ── Фикстуры ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Перенаправляет все DB-операции геокодера в изолированную временную БД."""
    import src.geocoder as geo
    monkeypatch.setattr(geo, "_DB_PATH", tmp_path / "test_cache.db")


@pytest.fixture
def with_key(monkeypatch):
    """Устанавливает тестовый API ключ."""
    monkeypatch.setenv("TWOGIS_API_KEY", "test-key-12345")


@pytest.fixture
def no_key(monkeypatch):
    """Гарантирует отсутствие API ключа."""
    monkeypatch.delenv("TWOGIS_API_KEY", raising=False)


def _make_api_response(items: list) -> MagicMock:
    """Создаёт мок ответа requests.get с заданными items."""
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = {"result": {"items": items, "total": len(items)}}
    return mock


def _make_api_response_403() -> MagicMock:
    mock = MagicMock()
    mock.status_code = 403
    return mock


def _make_api_response_empty() -> MagicMock:
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = {"result": {"items": [], "total": 0}}
    return mock


# ── _address_key ──────────────────────────────────────────────────────────────

def test_address_key_deterministic():
    """Одинаковый адрес → одинаковый ключ."""
    from src.geocoder import _address_key
    assert _address_key("Красный проспект, 1") == _address_key("Красный проспект, 1")


def test_address_key_case_insensitive():
    """Регистр не влияет на ключ."""
    from src.geocoder import _address_key
    assert _address_key("Ленина, 1") == _address_key("ленина, 1")


def test_address_key_whitespace_stripped():
    """Пробелы по краям не влияют на ключ."""
    from src.geocoder import _address_key
    assert _address_key("  Ленина, 1  ") == _address_key("Ленина, 1")


def test_address_key_is_md5():
    """Ключ — это MD5 нормализованной строки."""
    from src.geocoder import _address_key
    addr = "красный проспект, 25"
    expected = hashlib.md5(addr.encode()).hexdigest()
    assert _address_key("Красный проспект, 25") == expected


# ── extract_address ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("row,expected", [
    # parking / stops / schools / kindergartens / libraries / pharmacies (AdrStr)
    ({"AdrStreet": "ул. Ленина", "AdrDom": "5"},    "ул. Ленина, 5"),
    ({"AdrStreet": "пр. Маркса"},                   "пр. Маркса"),
    ({"AdrStr": "ул. Кирова", "AdrDom": "12"},      "ул. Кирова, 12"),
    # culture / sport_orgs (Ulica / Dom)
    ({"Ulica": "ул. Гоголя", "Dom": "3"},           "ул. Гоголя, 3"),
    ({"Ulica": "пер. Тихий"},                        "пер. Тихий"),
    # sport_grounds (Street / House)
    ({"Street": "ул. Есенина", "House": "7а"},       "ул. Есенина, 7а"),
    # приоритет: AdrStreet > AdrStr > Ulica > Street
    ({"AdrStreet": "ул. А", "Ulica": "ул. Б"},       "ул. А"),
    # нет ни одного поля → None
    ({"AdrDistr": "Советский район"},                None),
    ({},                                              None),
])
def test_extract_address(row, expected):
    from src.geocoder import extract_address
    assert extract_address(row) == expected


# ── geocode — graceful degradation без ключа ─────────────────────────────────

def test_geocode_no_key_returns_none(no_key):
    """Без ключа и без кеша geocode() возвращает None."""
    from src.geocoder import geocode
    result = geocode("Красный проспект, 1")
    assert result is None


def test_geocode_no_key_cache_hit(no_key):
    """Без ключа кеш всё равно работает."""
    from src.geocoder import geocode, _save_cache, _address_key, _ensure_table
    _ensure_table()
    addr = "новосибирск, красный проспект, 1"
    _save_cache(_address_key(addr), addr, 54.9884, 82.9090, "Красный пр., 1")

    result = geocode("Красный проспект, 1")
    assert result is not None
    assert result["source"] == "cache"
    assert result["lat"] == pytest.approx(54.9884)
    assert result["lon"] == pytest.approx(82.9090)


# ── geocode — попадание в кеш ─────────────────────────────────────────────────

def test_geocode_cache_hit_no_api_call(with_key):
    """При попадании в кеш HTTP-запрос к API не делается."""
    from src.geocoder import geocode, _save_cache, _address_key, _ensure_table
    _ensure_table()
    addr = "новосибирск, ул. ленина, 5"
    _save_cache(_address_key(addr), addr, 55.0, 83.0, "ул. Ленина, 5")

    with patch("requests.get") as mock_get:
        result = geocode("ул. Ленина, 5")
        mock_get.assert_not_called()

    assert result["source"] == "cache"


def test_geocode_cache_hit_returns_correct_coords(with_key):
    """Кеш возвращает сохранённые координаты."""
    from src.geocoder import geocode, _save_cache, _address_key, _ensure_table
    _ensure_table()
    addr = "новосибирск, вокзальная магистраль, 1"
    _save_cache(_address_key(addr), addr, 55.031, 82.907, "Вокзальная магистраль, 1")

    result = geocode("Вокзальная магистраль, 1")
    assert result["lat"] == pytest.approx(55.031)
    assert result["lon"] == pytest.approx(82.907)
    assert result["full_name"] == "Вокзальная магистраль, 1"


# ── geocode — успешный API-запрос ─────────────────────────────────────────────

def test_geocode_api_success_returns_coords(with_key):
    """Успешный API-ответ → координаты сохраняются в кеш и возвращаются."""
    from src.geocoder import geocode
    api_item = {
        "point": {"lat": 54.9884, "lon": 82.9090},
        "full_name": "Новосибирск, Красный проспект, 25",
    }
    with patch("requests.get", return_value=_make_api_response([api_item])):
        result = geocode("Красный проспект, 25")

    assert result is not None
    assert result["source"] == "api"
    assert result["lat"] == pytest.approx(54.9884)
    assert result["lon"] == pytest.approx(82.9090)
    assert "Красный проспект" in result["full_name"]


def test_geocode_api_result_saved_to_cache(with_key):
    """После API-запроса результат кешируется: повторный вызов не идёт в сеть."""
    from src.geocoder import geocode
    api_item = {"point": {"lat": 55.0, "lon": 83.0}, "full_name": "Тест"}

    with patch("requests.get", return_value=_make_api_response([api_item])) as mock_get:
        geocode("ул. Тестовая, 1")
        assert mock_get.call_count == 1

        geocode("ул. Тестовая, 1")
        assert mock_get.call_count == 1  # второй вызов из кеша


def test_geocode_api_adds_city_prefix(with_key):
    """Если город не упомянут в адресе, он добавляется как префикс."""
    from src.geocoder import geocode
    api_item = {"point": {"lat": 55.0, "lon": 83.0}, "full_name": "NSK"}

    with patch("requests.get", return_value=_make_api_response([api_item])) as mock_get:
        geocode("ул. Ленина, 1")

    call_params = mock_get.call_args[1]["params"]
    assert call_params["q"].startswith("Новосибирск")


def test_geocode_no_city_prefix_when_already_present(with_key):
    """Если адрес уже содержит город, дублирования нет."""
    from src.geocoder import geocode
    api_item = {"point": {"lat": 55.0, "lon": 83.0}, "full_name": "NSK"}

    with patch("requests.get", return_value=_make_api_response([api_item])) as mock_get:
        geocode("Новосибирск, ул. Ленина, 1")

    call_params = mock_get.call_args[1]["params"]
    assert call_params["q"].count("Новосибирск") == 1


# ── geocode — ошибки API ──────────────────────────────────────────────────────

def test_geocode_api_403_returns_none(with_key):
    """Просроченный/неверный ключ (403) → None, без исключения."""
    from src.geocoder import geocode
    with patch("requests.get", return_value=_make_api_response_403()):
        result = geocode("ул. Ленина, 1")
    assert result is None


def test_geocode_api_empty_items_returns_none(with_key):
    """Пустой список items в ответе → None."""
    from src.geocoder import geocode
    with patch("requests.get", return_value=_make_api_response_empty()):
        result = geocode("Несуществующая улица, 999")
    assert result is None


def test_geocode_timeout_returns_none(with_key):
    """Таймаут сети → None, без исключения."""
    import requests as _req
    from src.geocoder import geocode
    with patch("requests.get", side_effect=_req.exceptions.Timeout()):
        result = geocode("ул. Ленина, 1")
    assert result is None


def test_geocode_network_error_returns_none(with_key):
    """Любая другая ошибка сети → None."""
    from src.geocoder import geocode
    with patch("requests.get", side_effect=ConnectionError("no network")):
        result = geocode("ул. Ленина, 1")
    assert result is None


def test_geocode_missing_point_fields_returns_none(with_key):
    """Ответ без lat/lon в point → None."""
    from src.geocoder import geocode
    api_item = {"point": {}, "full_name": "Что-то"}
    with patch("requests.get", return_value=_make_api_response([api_item])):
        result = geocode("ул. Пустая, 0")
    assert result is None


# ── geocode_rows ──────────────────────────────────────────────────────────────

def test_geocode_rows_no_key_adds_none_coords(no_key):
    """Без ключа и без кеша строки получают _lat=None, _lon=None."""
    from src.geocoder import geocode_rows
    rows = [
        {"AdrStreet": "ул. Ленина", "AdrDom": "1", "Name": "Аптека 1"},
        {"AdrStreet": "пр. Маркса", "AdrDom": "5", "Name": "Аптека 2"},
    ]
    result = geocode_rows(rows)
    assert len(result) == 2
    assert result[0]["_lat"] is None
    assert result[0]["_lon"] is None
    assert result[0]["Name"] == "Аптека 1"


def test_geocode_rows_preserves_original_fields(no_key):
    """geocode_rows не изменяет существующие поля строки."""
    from src.geocoder import geocode_rows
    rows = [{"AdrStreet": "ул. Ленина", "AdrDom": "1", "Phone": "999"}]
    result = geocode_rows(rows)
    assert result[0]["Phone"] == "999"


def test_geocode_rows_no_address_fields(no_key):
    """Строки без адресных полей получают _lat=None, _lon=None."""
    from src.geocoder import geocode_rows
    rows = [{"BiblName": "Библиотека №1", "AdrDistr": "Советский район"}]
    result = geocode_rows(rows)
    assert result[0]["_lat"] is None
    assert result[0]["_lon"] is None


def test_geocode_rows_max_limit(no_key):
    """geocode_rows обрабатывает не более max_rows строк."""
    from src.geocoder import geocode_rows
    rows = [{"AdrStreet": f"ул. Тест, {i}"} for i in range(100)]
    result = geocode_rows(rows, max_rows=10)
    assert len(result) == 10


def test_geocode_rows_with_cache(with_key):
    """Строки с адресами из кеша получают реальные координаты."""
    from src.geocoder import geocode_rows, _save_cache, _address_key, _ensure_table
    _ensure_table()
    addr = "новосибирск, ул. кирова, 3"
    _save_cache(_address_key(addr), addr, 54.9, 82.85, "ул. Кирова, 3")

    rows = [{"AdrStreet": "ул. Кирова", "AdrDom": "3", "Name": "Школа №5"}]
    result = geocode_rows(rows)
    assert result[0]["_lat"] == pytest.approx(54.9)
    assert result[0]["_lon"] == pytest.approx(82.85)


# ── geocode_stats ─────────────────────────────────────────────────────────────

def test_geocode_stats_empty_db():
    """Пустой кеш → 0 адресов."""
    from src.geocoder import geocode_stats
    stats = geocode_stats()
    assert stats["cached_addresses"] == 0


def test_geocode_stats_after_save(with_key):
    """После геокодирования счётчик увеличивается."""
    from src.geocoder import geocode, geocode_stats
    api_item = {"point": {"lat": 55.0, "lon": 83.0}, "full_name": "NSK"}

    with patch("requests.get", return_value=_make_api_response([api_item])):
        geocode("ул. Счётная, 1")
        geocode("ул. Счётная, 2")

    stats = geocode_stats()
    assert stats["cached_addresses"] == 2


def test_geocode_stats_no_duplicate_on_repeat(with_key):
    """Повторный запрос одного адреса не дублирует запись в кеше."""
    from src.geocoder import geocode, geocode_stats
    api_item = {"point": {"lat": 55.0, "lon": 83.0}, "full_name": "NSK"}

    with patch("requests.get", return_value=_make_api_response([api_item])):
        geocode("ул. Уникальная, 7")
        geocode("ул. Уникальная, 7")

    stats = geocode_stats()
    assert stats["cached_addresses"] == 1
