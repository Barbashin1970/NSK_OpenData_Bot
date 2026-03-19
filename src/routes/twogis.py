"""2GIS API key management & geocoding endpoints."""

import json
import logging
import os
from pathlib import Path

from fastapi import APIRouter, Query

from ..city_config import get_city_name

log = logging.getLogger(__name__)

router = APIRouter()

_API_KEYS_FILE = Path(__file__).parent.parent.parent / "data" / "api_keys.json"


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


def _get_twogis_key() -> str | None:
    """Возвращает текущий ключ 2GIS: ENV > файл > None."""
    env_key = os.environ.get("TWOGIS_API_KEY", "").strip()
    if env_key:
        return env_key
    return _load_api_keys().get("twogis_key", "").strip() or None


@router.get(
    "/twogis/geocode",
    tags=["2GIS"],
    summary="Геокодировать адрес (адрес → координаты)",
    response_description="Координаты объекта или сообщение о недоступности",
)
def get_geocode(
    q: str = Query(..., description="Адрес для геокодирования. Например: `ул. Красный проспект, 25`"),
    city: str = Query(get_city_name(), description="Город (префикс к запросу)"),
) -> dict:
    """
    Конвертирует адресную строку в координаты через 2GIS Geocoder API.

    - Результаты кешируются в DuckDB (повторные запросы мгновенны, ключ не нужен).
    - Если ключ не задан и адрес не в кеше → `available: false`.
    - Используйте `GET /twogis/geocache-stats` чтобы увидеть размер кеша.
    """
    from ..geocoder import geocode

    key = _get_twogis_key()
    result = geocode(q, city)

    if result is None:
        if not key:
            return {"available": False, "reason": "2GIS ключ не задан. POST /twogis/key"}
        return {"available": True, "found": False, "query": q}

    return {
        "available": True,
        "found": True,
        "query": q,
        "lat": result["lat"],
        "lon": result["lon"],
        "full_name": result["full_name"],
        "source": result["source"],
    }


@router.get(
    "/twogis/geocache-stats",
    tags=["2GIS"],
    summary="Статистика кеша геокодирования",
)
def get_geocache_stats() -> dict:
    """Показывает количество адресов, сохранённых в кеше геокодирования."""
    from ..geocoder import geocode_stats
    return geocode_stats()


@router.get(
    "/twogis/key",
    tags=["2GIS"],
    summary="Текущий ключ 2GIS API",
    response_description="Ключ (маскированный), источник и статус",
)
def get_twogis_key_info() -> dict:
    """
    Возвращает информацию о текущем ключе 2GIS API.

    | Поле | Описание |
    |---|---|
    | `is_set` | `true` если ключ задан |
    | `masked` | Маскированное значение вида `2ea1a391...9092` |
    | `source` | Откуда взят ключ: `env` / `file` / `unset` |

    Для проверки валидности используйте `GET /twogis/validate`.
    """
    key = _get_twogis_key()
    if os.environ.get("TWOGIS_API_KEY", "").strip():
        source = "env"
    elif _load_api_keys().get("twogis_key", "").strip():
        source = "file"
    else:
        source = "unset"
    masked = f"{key[:8]}...{key[-4:]}" if key and len(key) > 12 else (key or "")
    return {
        "is_set": bool(key),
        "masked": masked,
        "source": source,
    }


@router.post(
    "/twogis/key",
    tags=["2GIS"],
    summary="Сохранить ключ 2GIS API",
    response_description="Подтверждение сохранения",
)
def set_twogis_key(
    key: str = Query(..., description="API ключ 2GIS. Получить: platform.2gis.ru"),
) -> dict:
    """
    Сохраняет ключ 2GIS API в `data/api_keys.json` и сразу применяет его в текущем процессе.

    - Если переменная окружения `TWOGIS_API_KEY` задана, она имеет приоритет над файлом.
    - После сохранения вызовите `GET /twogis/validate` для проверки.
    """
    key = key.strip()
    if not key:
        return {"ok": False, "error": "Ключ не может быть пустым"}
    keys = _load_api_keys()
    keys["twogis_key"] = key
    _save_api_keys(keys)
    os.environ["TWOGIS_API_KEY"] = key
    masked = f"{key[:8]}...{key[-4:]}" if len(key) > 12 else key
    return {"ok": True, "masked": masked, "source": "file"}


@router.get(
    "/twogis/validate",
    tags=["2GIS"],
    summary="Проверить валидность ключа 2GIS API",
    response_description="Результат проверки через Geocoder API",
)
def validate_twogis_key(
    key: str | None = Query(
        None,
        description="Ключ для проверки. Если не указан — используется текущий сохранённый.",
    ),
) -> dict:
    """
    Проверяет ключ 2GIS, выполняя тестовый запрос к Geocoder API
    (`catalog.api.2gis.com/3.0/items/geocode?q=Новосибирск`).

    | `valid` | Значение |
    |---|---|
    | `true` | Ключ рабочий, API отвечает |
    | `false` | Ключ неверный, истёк или сеть недоступна |
    """
    import requests as _req

    check_key = key.strip() if key else _get_twogis_key()
    if not check_key:
        return {"valid": False, "error": "Ключ не задан. Сохраните его через POST /twogis/key"}
    masked = f"{check_key[:8]}...{check_key[-4:]}" if len(check_key) > 12 else check_key
    try:
        resp = _req.get(
            "https://catalog.api.2gis.com/3.0/items/geocode",
            params={"q": "Новосибирск", "fields": "items.point", "key": check_key},
            timeout=10,
        )
        if resp.status_code == 200:
            total = resp.json().get("result", {}).get("total", 0)
            return {"valid": True, "masked": masked, "status_code": 200, "geocoder_results": total}
        elif resp.status_code == 403:
            return {"valid": False, "masked": masked, "error": "Ключ неверный или истёк", "status_code": 403}
        else:
            return {"valid": False, "masked": masked, "error": f"HTTP {resp.status_code}", "status_code": resp.status_code}
    except Exception as e:
        return {"valid": False, "masked": masked, "error": str(e)}


@router.get("/mapgl-key", include_in_schema=False)
def get_mapgl_key() -> dict:
    """Возвращает ключ 2GIS для инициализации MapGL JS на фронтенде."""
    key = _get_twogis_key()
    return {"key": key or "", "available": bool(key)}
