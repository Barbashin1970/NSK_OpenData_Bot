"""Разрешения на строительство и ввод в эксплуатацию (opendata.novo-sibirsk.ru).

Датасет 124 — разрешения на строительство (~5 942 записи)
  Поля: NomRazr, DatRazr, Zastr, NameOb, AdrOr, KadNom, Url

Датасет 125 — ввод в эксплуатацию (~1 935 записей)
  Поля: NomRazr, DatRazr, Zastr, NameOb, Raion, AdrOb, KadNom, KadNomMKD, DatMKD, Url

Логика «активных строек»:
  активные = записи из 124, чей KadNom отсутствует в 125
  Это объекты, на которые выдано разрешение, но ещё не оформлен ввод в эксплуатацию.
"""

import logging
from typing import Any

from .cache import _get_conn, table_exists

log = logging.getLogger(__name__)

_PERMITS_TABLE = "topic_construction_permits"
_COMMISSIONED_TABLE = "topic_construction_commissioned"

# Извлечение района из поля AdrOr — строится динамически из профиля города
def _district_case_sql() -> str:
    """Генерирует SQL CASE для извлечения района из AdrOr по активному city profile."""
    from .city_config import get_districts
    districts = get_districts()
    lines = []
    for name in districts:
        # Берём корень названия без " район"/" округ" для ILIKE
        stem = name.replace(" район", "").replace(" округ", "")
        lines.append(f"    WHEN \"AdrOr\" ILIKE '%{stem}%' THEN '{name}'")
    if not lines:
        return "''"
    return "CASE\n" + "\n".join(lines) + "\n    ELSE ''\nEND"


def permits_available() -> bool:
    return table_exists("construction_permits")


def commissioned_available() -> bool:
    return table_exists("construction_commissioned")


def query_active(
    district_filter: str | None = None,
    developer_filter: str | None = None,
    object_filter: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Активные стройки = разрешения - введённые в эксплуатацию (по KadNom).

    Возвращает (rows, total_count).
    """
    if not permits_available():
        return [], 0

    conn = _get_conn()
    try:
        if commissioned_available():
            base_cte = f"""WITH active AS (
                SELECT
                    "NomRazr", "DatRazr", "Zastr", "NameOb", "AdrOr", "KadNom",
                    {_district_case_sql()} AS district
                FROM {_PERMITS_TABLE}
                WHERE TRIM(COALESCE("KadNom", '')) = ''
                   OR TRIM("KadNom") NOT IN (
                       SELECT TRIM("KadNom")
                       FROM {_COMMISSIONED_TABLE}
                       WHERE TRIM(COALESCE("KadNom", '')) != ''
                   )
            )"""
        else:
            base_cte = f"""WITH active AS (
                SELECT
                    "NomRazr", "DatRazr", "Zastr", "NameOb", "AdrOr", "KadNom",
                    {_district_case_sql()} AS district
                FROM {_PERMITS_TABLE}
            )"""

        wheres: list[str] = []
        params: list[Any] = []

        if district_filter:
            wheres.append("district ILIKE ?")
            params.append(f"%{district_filter}%")
        if developer_filter:
            wheres.append('"Zastr" ILIKE ?')
            params.append(f"%{developer_filter}%")
        if object_filter:
            wheres.append('("NameOb" ILIKE ? OR "AdrOr" ILIKE ?)')
            params.extend([f"%{object_filter}%", f"%{object_filter}%"])

        where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""

        total = conn.execute(
            f"{base_cte} SELECT COUNT(*) FROM active {where_sql}", params
        ).fetchone()[0]

        data_sql = f"""{base_cte}
            SELECT "NomRazr", "DatRazr", "Zastr", "NameOb", "AdrOr", "KadNom", district
            FROM active
            {where_sql}
            ORDER BY "DatRazr" DESC
            LIMIT {limit} OFFSET {offset}
        """
        cursor = conn.execute(data_sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()], total

    except Exception as e:
        log.error(f"Ошибка query_active: {e}")
        return [], 0
    finally:
        conn.close()


def query_permits_list(
    permit_type: str = "permits",
    district_filter: str | None = None,
    developer_filter: str | None = None,
    object_filter: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Список разрешений или введённых объектов с фильтрами.

    permit_type: "permits" | "commissioned"
    Возвращает (rows, total_count).
    """
    if permit_type == "commissioned":
        if not commissioned_available():
            return [], 0
        table = _COMMISSIONED_TABLE
        conn = _get_conn()
        try:
            wheres: list[str] = []
            params: list[Any] = []
            if district_filter:
                wheres.append('"Raion" ILIKE ?')
                params.append(f"%{district_filter}%")
            if developer_filter:
                wheres.append('"Zastr" ILIKE ?')
                params.append(f"%{developer_filter}%")
            if object_filter:
                wheres.append('("NameOb" ILIKE ? OR "AdrOb" ILIKE ?)')
                params.extend([f"%{object_filter}%", f"%{object_filter}%"])

            where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
            total = conn.execute(
                f"SELECT COUNT(*) FROM {table} {where_sql}", params
            ).fetchone()[0]

            cursor = conn.execute(
                f"""SELECT "NomRazr", "DatRazr", "Zastr", "NameOb",
                           "Raion" AS district, "AdrOb" AS AdrOr, "KadNom"
                    FROM {table} {where_sql}
                    ORDER BY "DatRazr" DESC
                    LIMIT {limit} OFFSET {offset}""",
                params,
            )
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()], total
        except Exception as e:
            log.error(f"Ошибка query_permits_list commissioned: {e}")
            return [], 0
        finally:
            conn.close()

    else:  # permits
        if not permits_available():
            return [], 0
        conn = _get_conn()
        try:
            wheres: list[str] = []
            params: list[Any] = []
            if district_filter:
                wheres.append(f"({_district_case_sql()}) ILIKE ?")
                params.append(f"%{district_filter}%")
            if developer_filter:
                wheres.append('"Zastr" ILIKE ?')
                params.append(f"%{developer_filter}%")
            if object_filter:
                wheres.append('("NameOb" ILIKE ? OR "AdrOr" ILIKE ?)')
                params.extend([f"%{object_filter}%", f"%{object_filter}%"])

            where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
            total = conn.execute(
                f"SELECT COUNT(*) FROM {_PERMITS_TABLE} {where_sql}", params
            ).fetchone()[0]

            cursor = conn.execute(
                f"""SELECT "NomRazr", "DatRazr", "Zastr", "NameOb", "AdrOr",
                           ({_district_case_sql()}) AS district, "KadNom"
                    FROM {_PERMITS_TABLE} {where_sql}
                    ORDER BY "DatRazr" DESC
                    LIMIT {limit} OFFSET {offset}""",
                params,
            )
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()], total
        except Exception as e:
            log.error(f"Ошибка query_permits_list permits: {e}")
            return [], 0
        finally:
            conn.close()


def count_construction(
    permit_type: str = "active",
    district_filter: str | None = None,
) -> int:
    """Подсчёт строительных объектов.

    permit_type: "active" | "permits" | "commissioned"
    """
    if permit_type == "active":
        _, total = query_active(district_filter=district_filter, limit=1)
        return total

    if permit_type == "commissioned":
        if not commissioned_available():
            return 0
        conn = _get_conn()
        try:
            if district_filter:
                return conn.execute(
                    f'SELECT COUNT(*) FROM {_COMMISSIONED_TABLE} WHERE "Raion" ILIKE ?',
                    [f"%{district_filter}%"],
                ).fetchone()[0]
            return conn.execute(f"SELECT COUNT(*) FROM {_COMMISSIONED_TABLE}").fetchone()[0]
        except Exception as e:
            log.error(f"Ошибка count_construction commissioned: {e}")
            return 0
        finally:
            conn.close()

    else:  # permits
        if not permits_available():
            return 0
        conn = _get_conn()
        try:
            if district_filter:
                return conn.execute(
                    f"SELECT COUNT(*) FROM (SELECT ({_district_case_sql()}) AS d FROM {_PERMITS_TABLE}) WHERE d ILIKE ?",
                    [f"%{district_filter}%"],
                ).fetchone()[0]
            return conn.execute(f"SELECT COUNT(*) FROM {_PERMITS_TABLE}").fetchone()[0]
        except Exception as e:
            log.error(f"Ошибка count_construction permits: {e}")
            return 0
        finally:
            conn.close()


def group_by_district(permit_type: str = "active") -> list[dict]:
    """Группировка по районам.

    permit_type: "active" | "permits" | "commissioned"
    """
    conn = _get_conn()
    try:
        if permit_type == "commissioned":
            if not commissioned_available():
                return []
            sql = f"""
                SELECT "Raion" AS район, COUNT(*) AS количество
                FROM {_COMMISSIONED_TABLE}
                WHERE TRIM(COALESCE("Raion", '')) != ''
                GROUP BY "Raion"
                ORDER BY количество DESC
            """
        elif permit_type == "active" and commissioned_available():
            sql = f"""
                WITH active AS (
                    SELECT {_district_case_sql()} AS district
                    FROM {_PERMITS_TABLE}
                    WHERE TRIM(COALESCE("KadNom", '')) = ''
                       OR TRIM("KadNom") NOT IN (
                           SELECT TRIM("KadNom") FROM {_COMMISSIONED_TABLE}
                           WHERE TRIM(COALESCE("KadNom", '')) != ''
                       )
                )
                SELECT district AS район, COUNT(*) AS количество
                FROM active
                WHERE district != ''
                GROUP BY district
                ORDER BY количество DESC
            """
        else:  # permits (or active without commissioned table)
            if not permits_available():
                return []
            sql = f"""
                SELECT ({_district_case_sql()}) AS район, COUNT(*) AS количество
                FROM {_PERMITS_TABLE}
                WHERE ({_district_case_sql()}) != ''
                GROUP BY район
                ORDER BY количество DESC
            """

        cursor = conn.execute(sql)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка group_by_district: {e}")
        return []
    finally:
        conn.close()


def get_construction_meta() -> dict:
    """Метаданные: количество записей, дата обновления."""
    from .fetcher import load_meta

    fm = load_meta()
    meta: dict[str, Any] = {
        "permits_total": 0,
        "commissioned_total": 0,
        "active_total": 0,
        "permits_updated": "",
        "commissioned_updated": "",
    }

    if permits_available():
        conn = _get_conn()
        try:
            meta["permits_total"] = conn.execute(
                f"SELECT COUNT(*) FROM {_PERMITS_TABLE}"
            ).fetchone()[0]
        except Exception:
            pass
        finally:
            conn.close()
        meta["permits_updated"] = fm.get("construction_permits", {}).get("last_updated", "")

    if commissioned_available():
        conn = _get_conn()
        try:
            meta["commissioned_total"] = conn.execute(
                f"SELECT COUNT(*) FROM {_COMMISSIONED_TABLE}"
            ).fetchone()[0]
        except Exception:
            pass
        finally:
            conn.close()
        meta["commissioned_updated"] = fm.get("construction_commissioned", {}).get("last_updated", "")

    if meta["permits_total"] > 0:
        _, active = query_active(limit=1)
        meta["active_total"] = active

    return meta
