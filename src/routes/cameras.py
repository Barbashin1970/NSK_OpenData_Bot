"""Cameras (speed cameras from OSM) endpoints."""

import logging

from fastapi import APIRouter, Query

log = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/cameras",
    tags=["Камеры"],
    summary="Список камер фиксации нарушений ПДД",
    response_description="Массив камер с координатами (_lat, _lon) и мета-информацией",
)
def get_cameras(
    limit: int = Query(60, ge=1, le=200, description="Максимум записей в ответе"),
    district: str | None = Query(None, description="Фильтр по району (например 'Советский')"),
) -> dict:
    """
    Возвращает список стационарных камер фиксации нарушений ПДД в Новосибирске.

    Данные берутся из кеша OSM (Overpass API, тег `highway=speed_camera`).
    При первом запросе или по истечении TTL (7 дней) кеш обновляется автоматически.

    ### Поля каждой камеры

    | Поле | Тип | Описание |
    |---|---|---|
    | `osm_id` | string | ID объекта в OpenStreetMap |
    | `_lat` | float | Широта |
    | `_lon` | float | Долгота |
    | `maxspeed` | string | Ограничение скорости (например `60`) |
    | `name` | string | Название камеры (если задано в OSM) |
    | `direction` | string | Направление съёмки в градусах (если задано) |
    | `ref` | string | Номер / ссылка (если задано) |
    | `district` | string | Район города (вычисляется по координатам) |

    **Лицензия:** данные OpenStreetMap, ODbL — [openstreetmap.org/copyright](https://www.openstreetmap.org/copyright)

    Эквивалентно запросу: `GET /ask?q=камеры+видеофиксации`
    """
    from ..cameras_cache import query_cameras, count_cameras, get_cameras_meta, upsert_cameras, is_cameras_stale
    from ..cameras_fetcher import fetch_cameras

    if is_cameras_stale():
        fetched = fetch_cameras()
        if fetched:
            upsert_cameras(fetched)

    rows = query_cameras(limit=limit, district_filter=district)
    meta = get_cameras_meta()
    return {
        "operation": "FILTER",
        "count": count_cameras(district_filter=district),
        "rows": rows,
        "columns": ["osm_id", "_lat", "_lon", "maxspeed", "name", "direction", "ref", "district"],
        "coords_enriched": True,
        "coords_source": "OpenStreetMap (предзагружены)",
        "cameras_meta": {
            "last_updated": str(meta.get("last_updated") or ""),
            "total_rows": meta.get("total_rows", 0),
            "source": "OpenStreetMap · Overpass API · highway=speed_camera",
            "bbox": "54.70,82.60,55.25,83.40 (Новосибирск)",
            "license": "ODbL · openstreetmap.org/copyright",
        },
    }


@router.post(
    "/cameras/update",
    tags=["Камеры"],
    summary="Обновить данные о камерах фиксации нарушений (OSM)",
    response_description="Статус обновления: rows и success",
)
def post_cameras_update() -> dict:
    """
    Принудительно обновляет данные о стационарных камерах фиксации нарушений ПДД
    из OpenStreetMap через Overpass API.

    TTL: 7 дней. При запросах через `/ask?q=камеры` обновление происходит автоматически.

    **Источник:** OpenStreetMap, лицензия ODbL (openstreetmap.org/copyright).
    """
    from ..cameras_fetcher import fetch_cameras
    from ..cameras_cache import upsert_cameras, count_cameras
    cameras = fetch_cameras()
    if not cameras:
        existing = count_cameras()
        return {"updated": {"cameras": {
            "rows": existing,
            "success": existing > 0,
            "warning": "Overpass API недоступен — возвращены кешированные данные",
        }}}
    rows = upsert_cameras(cameras)
    return {"updated": {"cameras": {"rows": rows, "success": rows > 0}}}
