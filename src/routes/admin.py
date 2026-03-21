"""Admin endpoints: rules management, dev auth, city config."""

import hashlib as _hashlib
import json
import logging
import os
import re as _re
from pathlib import Path

from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.responses import JSONResponse

from ..city_config import (
    get_city_name, get_city_id, get_city_slug, get_districts,
    get_feature, get_opendata_base_url, get_sub_districts_info,
)

log = logging.getLogger(__name__)

router = APIRouter()

_RULES_DIR = Path(__file__).parent.parent.parent / "config" / "rules"
_ALLOWED_RULES = {"traffic_rules", "holiday_calendar", "ecology_rules", "life_indices_rules"}
_API_KEYS_FILE = Path(__file__).parent.parent.parent / "data" / "api_keys.json"

_DEV_DEFAULT_HASH = _hashlib.sha256(b"sigma2024").hexdigest()


def _load_api_keys() -> dict:
    try:
        if _API_KEYS_FILE.exists():
            return json.loads(_API_KEYS_FILE.read_text("utf-8"))
    except Exception:
        pass
    return {}


def _save_api_keys(keys: dict) -> None:
    _API_KEYS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _API_KEYS_FILE.write_text(json.dumps(keys, ensure_ascii=False, indent=2), "utf-8")


def _hash_pwd(pwd: str) -> str:
    return _hashlib.sha256(pwd.encode("utf-8")).hexdigest()


def _get_dev_hash() -> str:
    return _load_api_keys().get("dev_password_hash", _DEV_DEFAULT_HASH)


def _check_dev_pwd(pwd: str) -> bool:
    return _hash_pwd(pwd) == _get_dev_hash()


def _district_short_label(name: str) -> str:
    """'Кировский округ' → 'Кировский', 'Советский район' → 'Советский'."""
    return _re.sub(r"\s+(район|округ|р-н)$", "", name, flags=_re.IGNORECASE)


# ── Rules management ─────────────────────────────────────────────────────────

@router.post(
    "/admin/reload-rules",
    tags=["Администрирование"],
    summary="Горячая перезагрузка YAML-регламентов",
)
def admin_reload_rules() -> dict:
    """Перечитывает все YAML-файлы из `config/rules/` и пересобирает
    кэшированные глобалы в traffic_index.py.

    Применяет новые коэффициенты **без перезапуска сервера**.
    """
    from ..traffic_index import reload_traffic_rules
    reloaded = reload_traffic_rules()
    return {"status": "ok", "reloaded": reloaded}


@router.get(
    "/admin/rules-status",
    tags=["Администрирование"],
    summary="Статус кэша YAML-регламентов",
)
def admin_rules_status() -> dict:
    """Показывает, какие YAML-регламенты загружены в память и их версии."""
    from ..rule_engine import rules
    return rules.status()


@router.get(
    "/admin/rules/{name}",
    tags=["Администрирование"],
    summary="Получить YAML-регламент по имени",
)
def admin_get_rule(name: str) -> dict:
    """Возвращает содержимое YAML-регламента как текст и как разобранный dict."""
    if name not in _ALLOWED_RULES:
        raise HTTPException(status_code=404, detail=f"Регламент '{name}' не найден. Доступны: {sorted(_ALLOWED_RULES)}")
    path = _RULES_DIR / f"{name}.yaml"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Файл {path} не найден на диске")
    yaml_text = path.read_text(encoding="utf-8")
    import yaml as _yaml
    parsed = _yaml.safe_load(yaml_text) or {}
    return {"name": name, "yaml_text": yaml_text, "parsed": parsed}


@router.put(
    "/admin/rules/{name}",
    tags=["Администрирование"],
    summary="Сохранить YAML-регламент и применить без перезапуска",
)
async def admin_put_rule(name: str, request: Request) -> dict:
    """Принимает тело запроса с полем `yaml_text` (строка YAML), валидирует, сохраняет файл
    и вызывает горячую перезагрузку."""
    if name not in _ALLOWED_RULES:
        raise HTTPException(status_code=404, detail=f"Регламент '{name}' не найден")
    body = await request.json()
    yaml_text: str = body.get("yaml_text", "")
    if not yaml_text.strip():
        raise HTTPException(status_code=400, detail="Поле yaml_text не может быть пустым")

    import yaml as _yaml
    try:
        parsed = _yaml.safe_load(yaml_text)
    except _yaml.YAMLError as exc:
        raise HTTPException(status_code=400, detail=f"Синтаксическая ошибка YAML: {exc}")

    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="YAML должен быть маппингом (dict) верхнего уровня")

    path = _RULES_DIR / f"{name}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml_text, encoding="utf-8")

    from ..traffic_index import reload_traffic_rules
    reloaded = reload_traffic_rules()
    return {"status": "ok", "saved": name, "reloaded": reloaded}


# ── City config (for frontend) ───────────────────────────────────────────────

@router.get("/api/city-config", include_in_schema=False)
def city_config_endpoint():
    """Возвращает публичные параметры активного города для фронтенда и Studio."""
    from ..city_config import get_city_profile, get_ecology_stations as _eco_st

    profile = get_city_profile()
    features = profile.get("features", {})
    datasets = profile.get("static_datasets", {})

    districts_raw = get_districts()
    sub_districts = get_sub_districts_info()
    districts_list = [{"val": "", "label": "Весь город", "desc": "Сводные данные по всем районам"}]
    districts_list += [
        {"val": name, "label": _district_short_label(name), "desc": ""}
        for name in districts_raw.keys()
    ]
    for sd_name, (parent, _examples) in sub_districts.items():
        districts_list.append({
            "val": sd_name,
            "label": sd_name,
            "desc": _district_short_label(parent) + " р-н",
        })

    return {
        "city_id":    get_city_id(),
        "city_name":  get_city_name(),
        "city_name_genitive": get_city_name("genitive"),
        "city_name_prepositional": get_city_name("prepositional"),
        "city_slug":  get_city_slug(),
        "opendata_url": get_opendata_base_url(),
        "has_opendata_csv": bool(features.get("opendata_csv_enabled", False)),
        "power_outages_url":  features.get("power_outages_url", ""),
        "power_outages_base": features.get("power_outages_base", ""),
        "has_metro":   bool(features.get("has_metro")),
        "metro_name":  features.get("metro_name", ""),
        "has_airport": bool(features.get("has_airport")),
        "airport_name": features.get("airport_name", ""),
        "airport_iata": features.get("airport_iata", ""),
        "metro_lines":    datasets.get("metro", {}).get("lines_count", 0),
        "metro_stations": datasets.get("metro", {}).get("stations_count", 0),
        "ecology_stations_count": len(_eco_st()),
        "districts": districts_list,
        "static_datasets": {
            k: {"enabled": bool(v.get("enabled")), "note": v.get("note", "")}
            for k, v in datasets.items()
        },
    }


# ── Dev auth ──────────────────────────────────────────────────────────────────

@router.get("/dev-auth", include_in_schema=False)
def dev_auth(password: str = Query(...)):
    """Проверить пароль разработчика."""
    return {"valid": _check_dev_pwd(password)}


@router.post("/dev-password", include_in_schema=False)
async def dev_password_change(body: dict):
    """Изменить пароль разработчика."""
    old_pwd = body.get("old_password", "")
    new_pwd = body.get("new_password", "")
    if not _check_dev_pwd(old_pwd):
        return JSONResponse({"success": False, "detail": "Неверный текущий пароль"}, status_code=403)
    if len(new_pwd) < 6:
        return JSONResponse({"success": False, "detail": "Новый пароль слишком короткий"}, status_code=400)
    keys = _load_api_keys()
    keys["dev_password_hash"] = _hash_pwd(new_pwd)
    _save_api_keys(keys)
    return {"success": True}


# ── Available cities (for city-switcher in index.html) ────────────────────────

@router.get("/api/available-cities", include_in_schema=False)
def api_available_cities():
    """Список всех доступных профилей городов для выпадающего меню."""
    import yaml as _yaml
    _config_dir = Path(__file__).parent.parent.parent / "config"
    cities = []
    for p in sorted(_config_dir.glob("city_profile*.yaml")):
        try:
            with open(p, encoding="utf-8") as f:
                d = _yaml.safe_load(f)
            if d and "city" in d:
                c = d["city"]
                cities.append({"city_id": c.get("id", ""), "city_name": c.get("name", "")})
        except Exception:
            pass
    return {"cities": cities}


@router.post("/api/set-city", include_in_schema=False)
def api_set_city(body: dict):
    """Переключить активный город (делегирует studio_set_active_city)."""
    from .studio import studio_set_active_city
    return studio_set_active_city(body)


# ── District boundaries ─────────────────────────────────────────────────────

@router.post(
    "/admin/update-boundaries",
    tags=["Администрирование"],
    summary="Загрузить границы районов из OpenStreetMap",
)
def admin_update_boundaries() -> dict:
    """Загружает полигоны административных границ (районов/округов) из Overpass API
    и сохраняет в data/cities/{city_id}/district_boundaries.geojson.

    После этого классификация медучреждений и камер будет использовать
    точные полигоны вместо приблизительных центроидов.
    """
    from ..district_classifier import fetch_and_cache_boundaries
    return fetch_and_cache_boundaries()


@router.get(
    "/admin/boundaries-status",
    tags=["Администрирование"],
    summary="Статус файла границ районов",
)
def admin_boundaries_status() -> dict:
    """Показывает, есть ли файл границ и какие районы покрыты."""
    from ..district_classifier import _boundaries_path, _load_boundaries
    path = _boundaries_path()
    if not path.exists():
        return {
            "available": False,
            "path": str(path),
            "hint": "POST /admin/update-boundaries для загрузки",
        }
    boundaries = _load_boundaries()
    districts = sorted({b["district"] for b in boundaries}) if boundaries else []
    return {
        "available": True,
        "path": str(path),
        "districts": districts,
        "polygons_count": len(boundaries) if boundaries else 0,
    }
