"""Data Studio endpoints."""

import json
import logging
import os
import re as _re
from pathlib import Path

from fastapi import APIRouter, Query, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

log = logging.getLogger(__name__)

router = APIRouter()

_STATIC = Path(__file__).parent.parent / "static"
_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
_DATA_DIR = Path(__file__).parent.parent.parent / "data"


@router.get("/studio", include_in_schema=False)
def studio_page():
    """Data Studio — визуальный интерфейс управления городскими данными."""
    html_file = _STATIC / "studio.html"
    if not html_file.exists():
        return HTMLResponse("<h1>studio.html не найден</h1>", status_code=404)
    content = html_file.read_text(encoding="utf-8")
    return HTMLResponse(content, headers={"Cache-Control": "no-store"})


@router.get("/studio/api/profiles", include_in_schema=False)
def studio_profiles():
    """Список всех city_profile*.yaml с информацией о датасетах."""
    import yaml as _yaml

    profiles = []
    for yaml_path in sorted(_CONFIG_DIR.glob("city_profile*.yaml")):
        try:
            with open(yaml_path, encoding="utf-8") as f:
                data = _yaml.safe_load(f)
            city = data.get("city", {})
            static_ds = data.get("static_datasets", {})

            datasets_status = []
            for ds_name, ds_cfg in static_ds.items():
                enabled = ds_cfg.get("enabled", False)
                file_rel = ds_cfg.get("file", "")
                file_exists = False
                if enabled and file_rel:
                    file_exists = (Path(__file__).parent.parent.parent / file_rel).exists()
                datasets_status.append({
                    "name": ds_name,
                    "enabled": enabled,
                    "file": file_rel,
                    "file_exists": file_exists,
                })

            profiles.append({
                "profile_file": yaml_path.name,
                "city_id": city.get("id", ""),
                "city_name": city.get("name", ""),
                "city_name_genitive": city.get("name_genitive", ""),
                "timezone": city.get("timezone", ""),
                "utc_offset": city.get("utc_offset", 0),
                "center": city.get("center", {}),
                "districts_count": len(data.get("districts", {})),
                "features": data.get("features", {}),
                "static_datasets": datasets_status,
            })
        except Exception as exc:
            profiles.append({"profile_file": yaml_path.name, "error": str(exc)})

    return {"profiles": profiles}


@router.post("/studio/api/set-active-city", include_in_schema=False)
def studio_set_active_city(body: dict):
    """Переключить активный город без перезапуска сервера.

    Body: {"city_id": "omsk"}
    Меняет os.environ["CITY_PROFILE"] и сбрасывает lru_cache.
    """
    import yaml as _yaml
    from ..city_config import get_city_profile as _gcp, get_district_strip_re as _gdsr

    city_id = (body.get("city_id") or "").strip()
    if not city_id or not _re.match(r'^[\w\-]+$', city_id):
        raise HTTPException(status_code=400, detail="Недопустимый city_id")

    # Ищем yaml-файл по city_id
    matched_path = None
    for p in sorted(_CONFIG_DIR.glob("city_profile*.yaml")):
        try:
            with open(p, encoding="utf-8") as f:
                d = _yaml.safe_load(f)
            if d and d.get("city", {}).get("id") == city_id:
                matched_path = p
                break
        except Exception:
            continue

    if not matched_path:
        raise HTTPException(status_code=404, detail=f"Профиль для '{city_id}' не найден")

    profile_name = matched_path.stem
    os.environ["CITY_PROFILE"] = profile_name

    _gcp.cache_clear()
    try:
        _gdsr.cache_clear()
    except Exception:
        pass

    # Сбросить кэши статических датасетов (emissions, heat_sources, metro, airport)
    try:
        from ..emissions import load_emissions
        load_emissions.cache_clear()
    except Exception:
        pass
    try:
        from ..heat_sources import load_heat_sources, _load_geojson
        load_heat_sources.cache_clear()
        _load_geojson.cache_clear()
    except Exception:
        pass
    try:
        from ..metro_data import load_metro_stations
        load_metro_stations.cache_clear()
    except Exception:
        pass
    try:
        from ..airport_data import _load as _load_airport
        _load_airport.cache_clear()
    except Exception:
        pass

    new_profile = _gcp()
    city_name = new_profile.get("city", {}).get("name", city_id)

    try:
        from .. import router as _router
        from ..city_config import get_districts as _gd, get_sub_districts_compiled as _gsdc, get_sub_districts_info as _gsdi
        _router.DISTRICTS = _gd()
        _router._SUB_DISTRICTS = _gsdc()
        _router.SUB_DISTRICTS_INFO = _gsdi()
    except Exception:
        pass

    return {"ok": True, "city_id": city_id, "city_name": city_name, "profile_file": matched_path.name}


@router.get("/studio/api/schemas", include_in_schema=False)
def studio_schemas():
    """Канонические схемы из canonical_schemas.yaml."""
    import yaml as _yaml

    schemas_path = _CONFIG_DIR / "canonical_schemas.yaml"
    if not schemas_path.exists():
        return {"error": "canonical_schemas.yaml не найден"}
    with open(schemas_path, encoding="utf-8") as f:
        schemas = _yaml.safe_load(f)
    return schemas


@router.post("/studio/api/preview", include_in_schema=False)
async def studio_preview(file: UploadFile = File(...)):
    """Загрузить CSV/JSON → вернуть {columns, sample[5], format}."""
    import csv as _csv
    import tempfile as _tmp
    import os as _os

    content = await file.read()
    filename = file.filename or "upload"
    suffix = Path(filename).suffix.lower()

    with _tmp.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        if suffix in (".json", ".geojson"):
            raw = json.loads(content)
            if isinstance(raw, list):
                source_rows = raw
            elif isinstance(raw, dict):
                if "features" in raw:
                    source_rows = [
                        {
                            **f.get("properties", {}),
                            "_lon": (f.get("geometry") or {}).get("coordinates", [None, None])[0],
                            "_lat": (f.get("geometry") or {}).get("coordinates", [None, None])[1],
                        }
                        for f in raw["features"]
                    ]
                else:
                    source_rows = []
                    for v in raw.values():
                        if isinstance(v, list) and v:
                            source_rows = v
                            break
                    if not source_rows:
                        source_rows = [raw]
            else:
                source_rows = [raw]

            sample = source_rows[:5]
            columns = list(sample[0].keys()) if sample else []

        else:
            try:
                import duckdb as _ddb
                con = _ddb.connect()
                df = con.execute(
                    f"SELECT * FROM read_csv_auto('{tmp_path}') LIMIT 5"
                ).fetchdf()
                columns = list(df.columns)
                sample = df.to_dict(orient="records")
                con.close()
            except Exception:
                text = content.decode("utf-8-sig", errors="replace")
                reader = _csv.DictReader(text.splitlines())
                columns = list(reader.fieldnames or [])
                sample = [row for row, _ in zip(reader, range(5))]

        return {
            "filename": filename,
            "format": suffix.lstrip(".") or "csv",
            "columns": columns,
            "sample": sample,
        }
    finally:
        _os.unlink(tmp_path)


@router.get("/studio/api/profile-sources", include_in_schema=False)
def studio_profile_sources(city_id: str = Query(...)):
    """Возвращает конфиг онлайн-источников для конкретного профиля города."""
    import yaml as _yaml

    yaml_path = None
    for p in _CONFIG_DIR.glob("city_profile*.yaml"):
        try:
            d = _yaml.safe_load(p.read_text("utf-8"))
            if d.get("city", {}).get("id") == city_id:
                yaml_path = p
                break
        except Exception:
            continue

    if not yaml_path:
        raise HTTPException(status_code=404, detail=f"Профиль '{city_id}' не найден")

    with open(yaml_path, encoding="utf-8") as f:
        data = _yaml.safe_load(f)

    features = data.get("features", {})
    return {
        "city_id": city_id,
        "profile_file": yaml_path.name,
        "sources": [
            {
                "key":   "opendata_base_url",
                "label": "Портал открытых данных",
                "hint":  "Базовый URL городского opendata-портала (используется в ссылках и API docs)",
                "value": data.get("opendata_base_url", ""),
                "yaml_path": "top-level",
            },
            {
                "key":   "knowledge_base",
                "label": "База знаний",
                "hint":  "URL базы знаний / справочника (опционально)",
                "value": data.get("knowledge_base", ""),
                "yaml_path": "top-level",
            },
            {
                "key":   "power_outages_url",
                "label": "Отключения ЖКХ — страница",
                "hint":  "Полный URL страницы со списком отключений (аналог 051.novo-sibirsk.ru/sitepages/off.aspx)",
                "value": features.get("power_outages_url", ""),
                "yaml_path": "features",
            },
            {
                "key":   "power_outages_base",
                "label": "Отключения ЖКХ — базовый URL",
                "hint":  "Корневой URL сайта ЖКХ для построения относительных ссылок (аналог http://051.novo-sibirsk.ru)",
                "value": features.get("power_outages_base", ""),
                "yaml_path": "features",
            },
        ],
    }


@router.post("/studio/api/test-endpoint", include_in_schema=False)
async def studio_test_endpoint(request: Request):
    """Проверяет доступность URL — HTTP HEAD или GET с таймаутом 6 с."""
    import time as _time

    body = await request.json()
    url: str = (body.get("url") or "").strip()

    if not url:
        raise HTTPException(status_code=400, detail="url обязателен")
    if not url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="Разрешены только http:// и https:// URL")

    import requests as _req
    import urllib3 as _urllib3
    _urllib3.disable_warnings(_urllib3.exceptions.InsecureRequestWarning)

    _hdrs = {"User-Agent": "CityBot-Studio/1.1"}
    t0 = _time.monotonic()
    ssl_warning = None
    try:
        try:
            resp = _req.head(url, timeout=6, allow_redirects=True, headers=_hdrs, verify=True)
        except _req.exceptions.SSLError:
            resp = _req.head(url, timeout=6, allow_redirects=True, headers=_hdrs, verify=False)
            ssl_warning = "SSL-сертификат сайта не прошёл проверку — сайт доступен, но сертификат ненадёжен"
        if resp.status_code in (405, 501):
            try:
                resp = _req.get(url, timeout=6, stream=True, headers=_hdrs, verify=True)
            except _req.exceptions.SSLError:
                resp = _req.get(url, timeout=6, stream=True, headers=_hdrs, verify=False)
                ssl_warning = ssl_warning or "SSL-сертификат не прошёл проверку"
            resp.close()
        latency = int((_time.monotonic() - t0) * 1000)
        return {
            "ok": resp.status_code < 400,
            "status_code": resp.status_code,
            "latency_ms": latency,
            "error": ssl_warning,
        }
    except _req.exceptions.Timeout:
        return {"ok": False, "status_code": None, "latency_ms": 6000, "error": "Таймаут (>6 с)"}
    except _req.exceptions.ConnectionError as e:
        return {"ok": False, "status_code": None, "latency_ms": int((_time.monotonic()-t0)*1000), "error": f"Ошибка соединения: {e}"}
    except Exception as e:
        return {"ok": False, "status_code": None, "latency_ms": 0, "error": str(e)}


@router.post("/studio/api/save-online-source", include_in_schema=False)
async def studio_save_online_source(request: Request):
    """Сохраняет значение онлайн-источника в city_profile*.yaml, сохраняя комментарии."""
    import yaml as _yaml
    import re as _re2

    body = await request.json()
    city_id: str = (body.get("city_id") or "").strip()
    key: str = (body.get("key") or "").strip()
    value: str = (body.get("value") or "").strip()

    ALLOWED_KEYS = {"opendata_base_url", "knowledge_base", "power_outages_url", "power_outages_base"}
    if not city_id or not _re.match(r'^[\w\-]+$', city_id):
        raise HTTPException(status_code=400, detail="Недопустимый city_id")
    if key not in ALLOWED_KEYS:
        raise HTTPException(status_code=400, detail=f"Ключ '{key}' не поддерживается. Допустимые: {sorted(ALLOWED_KEYS)}")

    yaml_path = None
    for p in _CONFIG_DIR.glob("city_profile*.yaml"):
        try:
            d = _yaml.safe_load(p.read_text("utf-8"))
            if d.get("city", {}).get("id") == city_id:
                yaml_path = p
                break
        except Exception:
            continue

    if not yaml_path:
        raise HTTPException(status_code=404, detail=f"Профиль '{city_id}' не найден")

    content = yaml_path.read_text("utf-8")
    escaped_value = value.replace('"', '\\"')

    if key in ("opendata_base_url", "knowledge_base"):
        pattern = rf'^({_re.escape(key)}:\s*).*'
        replacement = rf'\g<1>"{escaped_value}"'
        new_content = _re2.sub(pattern, replacement, content, flags=_re2.MULTILINE)
    else:
        pattern = rf'^(  {_re.escape(key)}:\s*).*'
        replacement = rf'\g<1>"{escaped_value}"'
        new_content = _re2.sub(pattern, replacement, content, flags=_re2.MULTILINE)

    if new_content == content:
        return {"ok": False, "error": f"Поле '{key}' не найдено в {yaml_path.name}. Добавьте его вручную."}

    yaml_path.write_text(new_content, encoding="utf-8")
    return {"ok": True, "profile_file": yaml_path.name, "key": key, "value": value}


@router.post("/studio/api/import", include_in_schema=False)
async def studio_import(
    city_id: str = Form(...),
    dataset_type: str = Form(...),
    mapping: str = Form(...),
    file: UploadFile = File(...),
):
    """Загрузить файл + маппинг → сохранить в data/cities/<city_id>/<dataset_type>.json."""
    import csv as _csv
    import tempfile as _tmp
    import os as _os

    if not _re.match(r'^[\w\-]+$', city_id) or not _re.match(r'^[\w\-]+$', dataset_type):
        raise HTTPException(status_code=400, detail="Недопустимые символы в city_id или dataset_type")

    content = await file.read()
    filename = file.filename or "upload"
    suffix = Path(filename).suffix.lower()
    col_map: dict = json.loads(mapping)

    with _tmp.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        if suffix in (".json", ".geojson"):
            raw = json.loads(content)
            if isinstance(raw, list):
                source_rows = raw
            elif isinstance(raw, dict):
                if "features" in raw:
                    source_rows = [
                        {
                            **f.get("properties", {}),
                            "_lon": (f.get("geometry") or {}).get("coordinates", [None, None])[0],
                            "_lat": (f.get("geometry") or {}).get("coordinates", [None, None])[1],
                        }
                        for f in raw["features"]
                    ]
                else:
                    source_rows = []
                    for v in raw.values():
                        if isinstance(v, list) and v:
                            source_rows = v
                            break
                    if not source_rows:
                        source_rows = [raw]
            else:
                source_rows = [raw]
        else:
            try:
                import duckdb as _ddb
                con = _ddb.connect()
                df = con.execute(
                    f"SELECT * FROM read_csv_auto('{tmp_path}')"
                ).fetchdf()
                source_rows = df.to_dict(orient="records")
                con.close()
            except Exception:
                text = content.decode("utf-8-sig", errors="replace")
                source_rows = list(_csv.DictReader(text.splitlines()))

        mapped_rows = []
        for row in source_rows:
            mapped = {}
            for canonical, source_col in col_map.items():
                if source_col and source_col in row:
                    mapped[canonical] = row[source_col]
            if mapped:
                mapped_rows.append(mapped)

        out_dir = _DATA_DIR / "cities" / city_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{dataset_type}.json"
        out_file.write_text(
            json.dumps({"rows": mapped_rows}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return {
            "success": True,
            "city_id": city_id,
            "dataset_type": dataset_type,
            "rows_imported": len(mapped_rows),
            "saved_to": str(out_file.relative_to(Path(__file__).parent.parent.parent)),
        }
    finally:
        _os.unlink(tmp_path)
