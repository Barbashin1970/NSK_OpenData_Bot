"""Тесты целостности профилей городов и JSON-датасетов.

Запуск для всех профилей:
    pytest tests/test_city_profile.py -v

Добавление нового города:
    Достаточно создать config/city_profile_<id>.yaml — тест подхватит автоматически.
"""

import json
import os
import re
from pathlib import Path

import pytest
import yaml

# ── Конфигурация путей ────────────────────────────────────────────────────────

_ROOT = Path(__file__).parent.parent
_CONFIG_DIR = _ROOT / "config"

# ── Сбор всех профилей ────────────────────────────────────────────────────────

def _all_profiles() -> list[tuple[str, dict]]:
    """Возвращает [(yaml_name, profile_dict), ...] для всех city_profile*.yaml."""
    profiles = []
    for p in sorted(_CONFIG_DIR.glob("city_profile*.yaml")):
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        profiles.append((p.name, data))
    return profiles


_PROFILES = _all_profiles()
_PROFILE_IDS = [name for name, _ in _PROFILES]


# ── Хелперы ───────────────────────────────────────────────────────────────────

def _enabled_datasets(profile: dict) -> dict[str, dict]:
    """Возвращает только включённые датасеты с существующими файлами."""
    result = {}
    for name, ds in profile.get("static_datasets", {}).items():
        if ds.get("enabled") and ds.get("file"):
            result[name] = ds
    return result


# ── Тесты структуры city_profile ─────────────────────────────────────────────

@pytest.mark.parametrize("name,profile", _PROFILES, ids=_PROFILE_IDS)
class TestCityProfileStructure:

    def test_city_section_required_keys(self, name, profile):
        city = profile.get("city", {})
        for key in ("id", "name", "name_genitive", "name_prepositional",
                    "slug", "timezone", "utc_offset", "center", "bbox"):
            assert key in city, f"[{name}] city.{key} отсутствует"

    def test_city_center_coords(self, name, profile):
        center = profile["city"]["center"]
        assert "lat" in center and "lon" in center, f"[{name}] city.center: нужны lat и lon"
        assert 40 < center["lat"] < 80, f"[{name}] city.center.lat вне диапазона РФ"
        assert 20 < center["lon"] < 180, f"[{name}] city.center.lon вне диапазона РФ"

    def test_bbox_valid(self, name, profile):
        bb = profile["city"]["bbox"]
        for key in ("lat_min", "lat_max", "lon_min", "lon_max"):
            assert key in bb, f"[{name}] city.bbox.{key} отсутствует"
        assert bb["lat_min"] < bb["lat_max"], f"[{name}] bbox: lat_min >= lat_max"
        assert bb["lon_min"] < bb["lon_max"], f"[{name}] bbox: lon_min >= lon_max"

    def test_districts_non_empty(self, name, profile):
        districts = profile.get("districts", {})
        assert len(districts) >= 1, f"[{name}] districts пуст"
        for dname, stems in districts.items():
            assert isinstance(stems, list) and len(stems) >= 1, \
                f"[{name}] districts['{dname}']: нужен хотя бы один стем"

    def test_district_coords_consistent(self, name, profile):
        """Все ключи district_coords должны присутствовать в districts."""
        districts = set(profile.get("districts", {}).keys())
        coords = profile.get("district_coords", {})
        for dname in coords:
            assert dname in districts, \
                f"[{name}] district_coords['{dname}'] не найден в districts"

    def test_sub_districts_patterns_valid_regex(self, name, profile):
        for sd in profile.get("sub_districts", []):
            for pat in sd.get("patterns", []):
                try:
                    re.compile(pat)
                except re.error as e:
                    pytest.fail(f"[{name}] sub_districts '{sd['name']}' pattern '{pat}': {e}")

    def test_ecology_stations_required_fields(self, name, profile):
        stations = profile.get("ecology_stations", [])
        assert len(stations) >= 1, f"[{name}] ecology_stations пуст"
        for i, st in enumerate(stations):
            for key in ("station_id", "district", "lat", "lon"):
                assert key in st, f"[{name}] ecology_stations[{i}] нет поля '{key}'"
            assert 40 < st["lat"] < 80, f"[{name}] ecology_stations[{i}].lat вне РФ"
            assert 20 < st["lon"] < 180, f"[{name}] ecology_stations[{i}].lon вне РФ"

    def test_ecology_stations_district_match(self, name, profile):
        """district каждой станции должен быть в districts."""
        districts = set(profile.get("districts", {}).keys())
        for i, st in enumerate(profile.get("ecology_stations", [])):
            d = st.get("district", "")
            assert d in districts, \
                f"[{name}] ecology_stations[{i}].district='{d}' не в districts"

    def test_features_section_present(self, name, profile):
        assert "features" in profile, f"[{name}] секция features отсутствует"

    def test_static_datasets_section_present(self, name, profile):
        assert "static_datasets" in profile, f"[{name}] секция static_datasets отсутствует"

    def test_enabled_datasets_files_exist(self, name, profile):
        """Если enabled: true — файл должен существовать на диске."""
        for ds_name, ds in _enabled_datasets(profile).items():
            path = _ROOT / ds["file"]
            assert path.exists(), \
                f"[{name}] static_datasets.{ds_name}: файл не найден: {ds['file']}"


# ── Тесты содержимого JSON-датасетов ─────────────────────────────────────────

@pytest.mark.parametrize("name,profile", _PROFILES, ids=_PROFILE_IDS)
class TestCityDatasetContents:

    def _get_json(self, name, profile, ds_name) -> dict | list | None:
        ds = profile.get("static_datasets", {}).get(ds_name, {})
        if not ds.get("enabled") or not ds.get("file"):
            return None
        path = _ROOT / ds["file"]
        if not path.exists():
            return None
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    # ── metro.json ────────────────────────────────────────────────────────────

    def test_metro_json_structure(self, name, profile):
        data = self._get_json(name, profile, "metro")
        if data is None:
            pytest.skip("metro не включён")
        assert "info" in data, f"[{name}] metro.json: нет ключа 'info'"
        assert "lines" in data, f"[{name}] metro.json: нет ключа 'lines'"
        assert "stations" in data, f"[{name}] metro.json: нет ключа 'stations'"

    def test_metro_stations_count_matches_info(self, name, profile):
        data = self._get_json(name, profile, "metro")
        if data is None:
            pytest.skip("metro не включён")
        declared = data["info"].get("stations_count")
        actual   = len(data["stations"])
        assert declared == actual, \
            f"[{name}] metro.json: info.stations_count={declared} ≠ len(stations)={actual}"

    def test_metro_stations_required_fields(self, name, profile):
        data = self._get_json(name, profile, "metro")
        if data is None:
            pytest.skip("metro не включён")
        for i, st in enumerate(data["stations"]):
            for key in ("name", "line", "_lon", "_lat", "district"):
                assert key in st, f"[{name}] metro.json: stations[{i}] нет поля '{key}'"

    def test_metro_station_lines_declared(self, name, profile):
        data = self._get_json(name, profile, "metro")
        if data is None:
            pytest.skip("metro не включён")
        lines = set(data["lines"].keys())
        for i, st in enumerate(data["stations"]):
            assert st["line"] in lines, \
                f"[{name}] metro.json: stations[{i}].line='{st['line']}' не объявлена в lines"

    def test_metro_station_coords_in_bbox(self, name, profile):
        data = self._get_json(name, profile, "metro")
        if data is None:
            pytest.skip("metro не включён")
        bb = profile["city"]["bbox"]
        for i, st in enumerate(data["stations"]):
            assert bb["lat_min"] <= st["_lat"] <= bb["lat_max"], \
                f"[{name}] metro.json: stations[{i}] '{st['name']}': _lat вне bbox"
            assert bb["lon_min"] <= st["_lon"] <= bb["lon_max"], \
                f"[{name}] metro.json: stations[{i}] '{st['name']}': _lon вне bbox"

    # ── airport.json ──────────────────────────────────────────────────────────

    def test_airport_json_structure(self, name, profile):
        data = self._get_json(name, profile, "airport")
        if data is None:
            pytest.skip("airport не включён")
        for key in ("name", "iata", "_lon", "_lat", "terminals", "transport"):
            assert key in data, f"[{name}] airport.json: нет ключа '{key}'"

    def test_airport_terminals_non_empty(self, name, profile):
        data = self._get_json(name, profile, "airport")
        if data is None:
            pytest.skip("airport не включён")
        assert len(data["terminals"]) >= 1, f"[{name}] airport.json: terminals пуст"

    def test_airport_coords_in_bbox(self, name, profile):
        data = self._get_json(name, profile, "airport")
        if data is None:
            pytest.skip("airport не включён")
        bb = profile["city"]["bbox"]
        # Аэропорт может быть за пределами административной границы города
        # (например, Толмачёво в г. Обь) — допускаем 1° отклонение
        margin = 1.0
        assert bb["lat_min"] - margin <= data["_lat"] <= bb["lat_max"] + margin, \
            f"[{name}] airport.json: _lat={data['_lat']} далеко от bbox города"
        assert bb["lon_min"] - margin <= data["_lon"] <= bb["lon_max"] + margin, \
            f"[{name}] airport.json: _lon={data['_lon']} далеко от bbox города"

    # ── emissions.json ────────────────────────────────────────────────────────

    def test_emissions_json_structure(self, name, profile):
        data = self._get_json(name, profile, "emissions")
        if data is None:
            pytest.skip("emissions не включён")
        assert "municipalities" in data, f"[{name}] emissions JSON: нет ключа 'municipalities'"
        assert len(data["municipalities"]) >= 1, f"[{name}] emissions JSON: municipalities пуст"

    def test_emissions_municipalities_required_fields(self, name, profile):
        data = self._get_json(name, profile, "emissions")
        if data is None:
            pytest.skip("emissions не включён")
        for i, m in enumerate(data["municipalities"]):
            for key in ("id", "name", "lat", "lon", "vsego_t"):
                assert key in m, \
                    f"[{name}] emissions JSON: municipalities[{i}] нет поля '{key}'"

    # ── heat_sources.geojson ──────────────────────────────────────────────────

    def test_heat_sources_geojson_structure(self, name, profile):
        ds = profile.get("static_datasets", {}).get("heat_sources", {})
        if not ds.get("enabled") or not ds.get("file"):
            pytest.skip("heat_sources не включён")
        path = _ROOT / ds["file"]
        if not path.exists():
            pytest.skip(f"файл не найден: {ds['file']}")
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        assert data.get("type") == "FeatureCollection", \
            f"[{name}] heat_sources: ожидается GeoJSON FeatureCollection"
        assert "features" in data and len(data["features"]) >= 1, \
            f"[{name}] heat_sources: features пуст"

    def test_heat_sources_required_properties(self, name, profile):
        ds = profile.get("static_datasets", {}).get("heat_sources", {})
        if not ds.get("enabled") or not ds.get("file"):
            pytest.skip("heat_sources не включён")
        path = _ROOT / ds["file"]
        if not path.exists():
            pytest.skip(f"файл не найден: {ds['file']}")
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        required = ("id", "short_name", "type", "fuel", "operator_group", "district")
        for i, feat in enumerate(data["features"]):
            props = feat.get("properties", {})
            for key in required:
                assert key in props, \
                    f"[{name}] heat_sources: features[{i}].properties нет '{key}'"


# ── Тест: смена города сбрасывает кэш выбросов ──────────────────────────────

def _emissions_cities() -> list[tuple[str, str]]:
    """Возвращает [(city_id, profile_stem), ...] для городов с включёнными выбросами."""
    result = []
    for p in sorted(_CONFIG_DIR.glob("city_profile*.yaml")):
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        ds = data.get("static_datasets", {}).get("emissions", {})
        if ds.get("enabled") and ds.get("file"):
            city_id = data.get("city", {}).get("id", "")
            result.append((city_id, p.stem))
    return result


_EMISSION_CITIES = _emissions_cities()


@pytest.mark.skipif(len(_EMISSION_CITIES) < 2, reason="Нужно ≥ 2 города с выбросами")
class TestCitySwitchEmissionsCache:
    """При смене города load_emissions() должен возвращать данные нового города."""

    def test_emissions_data_changes_on_city_switch(self):
        from src.city_config import get_city_profile
        from src.emissions import load_emissions

        city_a_id, city_a_profile = _EMISSION_CITIES[0]
        city_b_id, city_b_profile = _EMISSION_CITIES[1]

        old_env = os.environ.get("CITY_PROFILE")
        try:
            # Загружаем выбросы города A
            os.environ["CITY_PROFILE"] = city_a_profile
            get_city_profile.cache_clear()
            load_emissions.cache_clear()
            data_a = load_emissions()

            # Переключаемся на город B
            os.environ["CITY_PROFILE"] = city_b_profile
            get_city_profile.cache_clear()
            load_emissions.cache_clear()
            data_b = load_emissions()

            # Данные должны отличаться
            names_a = {r["name"] for r in data_a}
            names_b = {r["name"] for r in data_b}
            assert names_a != names_b, (
                f"Выбросы {city_a_id} и {city_b_id} идентичны — "
                f"кэш не сбросился или файлы совпадают"
            )
        finally:
            # Восстановить ENV
            if old_env is not None:
                os.environ["CITY_PROFILE"] = old_env
            else:
                os.environ.pop("CITY_PROFILE", None)
            get_city_profile.cache_clear()
            load_emissions.cache_clear()

    def test_set_city_endpoint_clears_emissions_cache(self):
        """POST /api/set-city должен сбросить кэш load_emissions."""
        from fastapi.testclient import TestClient
        from src.api import app
        from src.city_config import get_city_profile, get_districts, \
            get_sub_districts_compiled, get_sub_districts_info
        from src.emissions import load_emissions
        from src import router as _router

        client = TestClient(app)

        city_a_id, city_a_profile = _EMISSION_CITIES[0]
        city_b_id, city_b_profile = _EMISSION_CITIES[1]

        old_env = os.environ.get("CITY_PROFILE")
        old_districts = _router.DISTRICTS
        old_sub_districts = _router._SUB_DISTRICTS
        old_sub_info = _router.SUB_DISTRICTS_INFO
        try:
            # Устанавливаем город A
            os.environ["CITY_PROFILE"] = city_a_profile
            get_city_profile.cache_clear()
            load_emissions.cache_clear()
            data_a = load_emissions()

            # Переключаемся через API
            resp = client.post("/api/set-city", json={"city_id": city_b_id})
            assert resp.status_code == 200

            # load_emissions() должен вернуть данные города B
            data_after = load_emissions()
            names_a = {r["name"] for r in data_a}
            names_after = {r["name"] for r in data_after}
            assert names_a != names_after, (
                f"После set-city({city_b_id}) выбросы не изменились — "
                f"кэш load_emissions не сброшен"
            )
        finally:
            if old_env is not None:
                os.environ["CITY_PROFILE"] = old_env
            else:
                os.environ.pop("CITY_PROFILE", None)
            get_city_profile.cache_clear()
            load_emissions.cache_clear()
            # Восстановить глобальное состояние router
            _router.DISTRICTS = old_districts
            _router._SUB_DISTRICTS = old_sub_districts
            _router.SUB_DISTRICTS_INFO = old_sub_info
