"""Medical facilities (from OSM) endpoints."""

import logging

from fastapi import APIRouter, Query

log = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/medical",
    tags=["Медицина"],
    summary="Список медицинских учреждений Новосибирска",
    response_description="Массив учреждений с координатами (_lat, _lon) и мета-информацией",
)
def get_medical(
    limit: int = Query(50, ge=1, le=200, description="Максимум записей в ответе"),
    district: str | None = Query(None, description="Фильтр по району (например 'Советский')"),
    facility_type: str | None = Query(None, description="Тип: hospital или clinic"),
) -> dict:
    """
    Возвращает список больниц и поликлиник Новосибирска из OSM (Overpass API).

    При первом запросе или по истечении TTL (72 часа) кеш обновляется автоматически.

    ### Поля каждого объекта

    | Поле | Тип | Описание |
    |---|---|---|
    | `osm_id` | string | ID объекта в OpenStreetMap (n/w-префикс) |
    | `name` | string | Название учреждения |
    | `facility_type` | string | `hospital` или `clinic` |
    | `type_label` | string | Читаемое название типа |
    | `emergency` | string | Наличие приёмного покоя |
    | `phone` | string | Телефон (если задан в OSM) |
    | `address` | string | Адрес (улица + номер дома) |
    | `district` | string | Район города (вычисляется по координатам) |
    | `_lat` | float | Широта |
    | `_lon` | float | Долгота |

    **Лицензия:** данные OpenStreetMap, ODbL — [openstreetmap.org/copyright](https://www.openstreetmap.org/copyright)
    """
    from ..medical_cache import query_medical, count_medical, get_medical_meta, upsert_medical, is_medical_stale
    from ..medical_fetcher import fetch_medical

    if is_medical_stale():
        fetched = fetch_medical()
        if fetched:
            upsert_medical(fetched)

    rows = query_medical(limit=limit, district_filter=district, type_filter=facility_type)
    meta = get_medical_meta()
    return {
        "operation": "FILTER",
        "count": count_medical(district_filter=district, type_filter=facility_type),
        "rows": rows,
        "columns": ["osm_id", "name", "facility_type", "type_label", "emergency", "phone", "address", "district", "_lat", "_lon"],
        "coords_enriched": True,
        "coords_source": "OpenStreetMap (предзагружены)",
        "medical_meta": {
            "last_updated": str(meta.get("last_updated") or ""),
            "total_rows": meta.get("total_rows", 0),
            "source": "OpenStreetMap · Overpass API · amenity=hospital|clinic",
            "bbox": "54.70,82.60,55.25,83.40 (Новосибирск)",
            "license": "ODbL · openstreetmap.org/copyright",
        },
    }


@router.post(
    "/medical/update",
    tags=["Медицина"],
    summary="Обновить данные о медицинских учреждениях (OSM)",
    response_description="Статус обновления: rows и success",
)
def post_medical_update() -> dict:
    """
    Принудительно обновляет данные о больницах и поликлиниках Новосибирска
    из OpenStreetMap через Overpass API.

    TTL: 72 часа. При запросах через `/ask?q=больницы` обновление происходит автоматически.

    **Источник:** OpenStreetMap, лицензия ODbL (openstreetmap.org/copyright).
    """
    from ..medical_fetcher import fetch_medical
    from ..medical_cache import upsert_medical, count_medical
    facilities = fetch_medical()
    if not facilities:
        existing = count_medical()
        return {"updated": {"medical": {
            "rows": existing,
            "success": existing > 0,
            "warning": "Overpass API недоступен — возвращены кешированные данные",
        }}}
    rows = upsert_medical(facilities)
    return {"updated": {"medical": {"rows": rows, "success": rows > 0}}}
