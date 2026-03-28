"""Хранилище данных об отключениях ЖКХ в DuckDB.

Таблица power_outages — скользящее окно:
  - история за последние POWER_HISTORY_DAYS дней
  - данные хранятся как временные снимки (scraped_at = timestamp)

Каждый запуск fetch_all_outages() добавляет новую группу записей —
накапливается история изменений состояния систем ЖКХ.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .cache import _get_conn          # единственный _get_conn на весь проект
from .constants import (
    DATA_DIR, POWER_HISTORY_DAYS, POWER_TTL_MINUTES
)

log = logging.getLogger(__name__)

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS power_outages (
    id            VARCHAR,
    utility       VARCHAR,
    utility_id    VARCHAR,
    group_type    VARCHAR,
    district      VARCHAR,
    district_href VARCHAR,
    houses        INTEGER,
    scraped_at    VARCHAR,
    source_url    VARCHAR,
    date_from     VARCHAR,
    date_to       VARCHAR
)
"""

_DAILY_ARCHIVE_DDL = """
CREATE TABLE IF NOT EXISTS power_daily_archive (
    day             VARCHAR,
    district        VARCHAR,
    utility         VARCHAR,
    active_houses   INTEGER DEFAULT 0,
    planned_houses  INTEGER DEFAULT 0,
    active_records  INTEGER DEFAULT 0,
    planned_records INTEGER DEFAULT 0,
    snapshots       INTEGER DEFAULT 0,
    PRIMARY KEY (day, district, utility)
)
"""

_DETAIL_DDL = """
CREATE TABLE IF NOT EXISTS power_outages_detail (
    id            VARCHAR,
    utility_id    VARCHAR,
    district_href VARCHAR,
    address       VARCHAR,
    date_from     VARCHAR,
    date_to       VARCHAR,
    reason        VARCHAR,
    scraped_at    VARCHAR,
    source_url    VARCHAR
)
"""


def init_power_table() -> None:
    """Создаёт таблицы power_outages, power_outages_detail и power_daily_archive."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = _get_conn()
    try:
        conn.execute(_TABLE_DDL)
        conn.execute(_DETAIL_DDL)
        conn.execute(_DAILY_ARCHIVE_DDL)
        # Миграция: добавляем date_from/date_to если их ещё нет (старая схема)
        for col in ("date_from", "date_to"):
            try:
                conn.execute(f"ALTER TABLE power_outages ADD COLUMN {col} VARCHAR")
            except Exception:
                pass  # колонка уже существует
    finally:
        conn.close()


def _save_power_daily_archive(conn) -> None:
    """Пересчитывает и сохраняет агрегаты за сегодня в power_daily_archive.

    Вызывается после каждого upsert — аналогично ecology_cache._save_daily_archive.
    Архив хранится 365 дней (не зависит от POWER_HISTORY_DAYS).
    """
    try:
        conn.execute("""
            DELETE FROM power_daily_archive
            WHERE day < STRFTIME(CURRENT_DATE - INTERVAL '365 days', '%Y-%m-%d')
        """)
        conn.execute("""
            INSERT INTO power_daily_archive
                (day, district, utility,
                 active_houses, planned_houses,
                 active_records, planned_records, snapshots)
            SELECT
                STRFTIME(CAST(scraped_at AS TIMESTAMP), '%Y-%m-%d') AS day,
                district,
                utility,
                COALESCE(SUM(CASE WHEN group_type = 'active'  THEN houses ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN group_type = 'planned' THEN houses ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN group_type = 'active'  THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN group_type = 'planned' THEN 1 ELSE 0 END), 0),
                COUNT(DISTINCT scraped_at)
            FROM power_outages
            WHERE STRFTIME(CAST(scraped_at AS TIMESTAMP), '%Y-%m-%d')
                  = STRFTIME(CURRENT_DATE, '%Y-%m-%d')
              AND district != 'all'
            GROUP BY day, district, utility
            ON CONFLICT (day, district, utility) DO UPDATE SET
                active_houses   = excluded.active_houses,
                planned_houses  = excluded.planned_houses,
                active_records  = excluded.active_records,
                planned_records = excluded.planned_records,
                snapshots       = excluded.snapshots
        """)
    except Exception as e:
        log.error("_save_power_daily_archive error: %s", e)


def upsert_outages(records: list[dict[str, Any]]) -> int:
    """Вставляет записи в power_outages с автоматической очисткой старых данных.

    Возвращает количество добавленных записей.
    """
    if not records:
        return 0

    init_power_table()
    conn = _get_conn()
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=POWER_HISTORY_DAYS)
        ).isoformat()
        conn.execute("DELETE FROM power_outages WHERE scraped_at < ?", [cutoff])

        rows = [
            (
                r["id"],
                r["utility"],
                r["utility_id"],
                r["group_type"],
                r["district"],
                r.get("district_href", ""),
                int(r.get("houses", 0)),
                r["scraped_at"],
                r.get("source_url", ""),
                r.get("date_from"),
                r.get("date_to"),
            )
            for r in records
        ]
        conn.executemany("INSERT INTO power_outages VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
        _save_power_daily_archive(conn)
        log.info(f"Добавлено {len(rows)} записей в power_outages")
        return len(rows)
    finally:
        conn.close()


def is_power_stale(ttl_minutes: int = POWER_TTL_MINUTES) -> bool:
    """Возвращает True если данные устарели или таблицы нет."""
    try:
        init_power_table()
        conn = _get_conn()
        try:
            result = conn.execute("SELECT MAX(scraped_at) FROM power_outages").fetchone()
            last = result[0] if result else None
            if not last:
                return True
            last_dt = datetime.fromisoformat(last)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) - last_dt > timedelta(minutes=ttl_minutes)
        finally:
            conn.close()
    except Exception:
        return True


def get_power_meta(
    utility_filter: str | None = None,
    district_filter: str | None = None,
) -> dict:
    """Возвращает метаданные: последнее обновление, кол-во записей, активных/плановых домов.

    utility_filter  — если задан (например "электроснабж"), считает только этот тип ресурса.
    district_filter — если задан (например "Ленинский"), считает только этот район.
    Оба фильтра должны совпадать с теми, что переданы в query_power, чтобы цифры в шапке
    совпадали с числами строк таблицы.
    """
    try:
        init_power_table()
        conn = _get_conn()
        try:
            last = conn.execute("SELECT MAX(scraped_at) FROM power_outages").fetchone()[0] or ""
            total = conn.execute("SELECT COUNT(*) FROM power_outages").fetchone()[0]
            latest_cond = "scraped_at = (SELECT MAX(scraped_at) FROM power_outages)"
            extra_conds: list[str] = []
            extra_params: list[str] = []
            if utility_filter:
                extra_conds.append("utility ILIKE ?")
                extra_params.append(f"%{utility_filter}%")
            if district_filter:
                extra_conds.append("district ILIKE ?")
                extra_params.append(f"%{district_filter}%")
            extra_sql = (" AND " + " AND ".join(extra_conds)) if extra_conds else ""
            active = conn.execute(
                f"SELECT COALESCE(SUM(houses), 0) FROM power_outages"
                f" WHERE {latest_cond} AND group_type='active'{extra_sql}",
                extra_params,
            ).fetchone()[0]
            planned = conn.execute(
                f"SELECT COALESCE(SUM(houses), 0) FROM power_outages"
                f" WHERE {latest_cond} AND group_type='planned'{extra_sql}",
                extra_params,
            ).fetchone()[0]
            return {
                "last_scraped": last,
                "total_records": total,
                "active_houses": int(active),
                "planned_houses": int(planned),
            }
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Ошибка get_power_meta: {e}")
        return {"last_scraped": "", "total_records": 0, "active_houses": 0, "planned_houses": 0}


def query_power(
    utility_filter: str | None = None,
    district_filter: str | None = None,
    group_filter: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    latest_only: bool = False,
) -> list[dict]:
    """Запрос к power_outages с фильтрами."""
    init_power_table()
    conn = _get_conn()
    try:
        wheres: list[str] = []
        params: list = []

        if latest_only:
            wheres.append("scraped_at = (SELECT MAX(scraped_at) FROM power_outages)")
        if utility_filter:
            wheres.append("utility ILIKE ?")
            params.append(f"%{utility_filter}%")
        if district_filter:
            wheres.append("district ILIKE ?")
            params.append(f"%{district_filter}%")
        if group_filter:
            wheres.append("group_type = ?")
            params.append(group_filter)
        if date_from:
            wheres.append("scraped_at >= ?")
            params.append(date_from)
        if date_to:
            wheres.append("scraped_at <= ?")
            params.append(date_to)

        where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        sql = f"""
            SELECT utility, utility_id, group_type, district, houses, scraped_at, source_url,
                   date_from, date_to
            FROM power_outages
            {where_sql}
            ORDER BY scraped_at DESC, utility, district
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка query_power: {e}")
        return []
    finally:
        conn.close()


def get_history_by_day(
    utility_filter: str | None = None,
    district_filter: str | None = None,
    days: int = 7,
) -> list[dict]:
    """Сводная история по дням за последние N дней (пик по домам в день)."""
    init_power_table()
    conn = _get_conn()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        wheres = ["scraped_at >= ?"]
        params: list = [cutoff]

        if utility_filter:
            wheres.append("utility ILIKE ?")
            params.append(f"%{utility_filter}%")
        if district_filter:
            wheres.append("district ILIKE ?")
            params.append(f"%{district_filter}%")

        where_sql = "WHERE " + " AND ".join(wheres)
        sql = f"""
            SELECT
                STRFTIME(CAST(scraped_at AS TIMESTAMP), '%Y-%m-%d') AS day,
                group_type,
                SUM(houses)              AS total_houses,
                COUNT(DISTINCT scraped_at) AS snapshots
            FROM power_outages
            {where_sql}
            GROUP BY day, group_type
            ORDER BY day DESC, group_type
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка get_history_by_day: {e}")
        return []
    finally:
        conn.close()


def get_current_status() -> list[dict]:
    """Текущий статус по всем утилитам из последнего скрапа."""
    return query_power(latest_only=True)


def get_electricity_status(district_filter: str | None = None) -> list[dict]:
    """Статус электроснабжения из последнего скрапа."""
    return query_power(
        utility_filter="электроснабж",
        district_filter=district_filter,
        latest_only=True,
    )


def upsert_detail(records: list[dict]) -> int:
    """Вставляет детальные записи об отключениях (адреса) с очисткой старых."""
    if not records:
        return 0
    init_power_table()
    conn = _get_conn()
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=POWER_HISTORY_DAYS)
        ).isoformat()
        conn.execute("DELETE FROM power_outages_detail WHERE scraped_at < ?", [cutoff])
        rows = [
            (
                r["id"], r["utility_id"], r.get("district_href", ""),
                r["address"], r.get("date_from", ""), r.get("date_to", ""),
                r.get("reason", ""), r["scraped_at"], r.get("source_url", ""),
            )
            for r in records
        ]
        conn.executemany(
            "INSERT INTO power_outages_detail VALUES (?,?,?,?,?,?,?,?,?)", rows
        )
        return len(rows)
    finally:
        conn.close()


def query_power_addresses(
    utility_id: str | None = None,
    address_contains: str | None = None,
    latest_only: bool = True,
    limit: int = 50,
) -> list[dict]:
    """Запрашивает детальные адресные записи из power_outages_detail."""
    init_power_table()
    conn = _get_conn()
    try:
        wheres: list[str] = []
        params: list = []
        if latest_only:
            wheres.append(
                "scraped_at = (SELECT MAX(scraped_at) FROM power_outages_detail)"
            )
        if utility_id:
            wheres.append("utility_id = ?")
            params.append(utility_id)
        if address_contains:
            wheres.append("address ILIKE ?")
            params.append(f"%{address_contains}%")
        where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        sql = f"""
            SELECT utility_id, address, date_from, date_to, reason, scraped_at
            FROM power_outages_detail
            {where_sql}
            ORDER BY scraped_at DESC, address
            LIMIT {limit}
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка query_power_addresses: {e}")
        return []
    finally:
        conn.close()


def query_power_history(
    district_filter: str | None = None,
    utility_filter: str | None = None,
    days: int = 30,
) -> list[dict]:
    """30-дневная история отключений: raw + archive (аналог ecology query_history).

    Возвращает строки с ключами:
      day, district, utility, active_houses, planned_houses,
      active_records, planned_records, snapshots
    """
    init_power_table()
    conn = _get_conn()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

        # --- Recent: aggregate from power_outages (raw snapshots) ---
        r_wheres = ["STRFTIME(CAST(scraped_at AS TIMESTAMP), '%Y-%m-%d') >= ?"]
        r_params: list = [cutoff]
        # Исключаем артефакт "all" — это не район
        r_wheres.append("district != 'all'")
        if district_filter:
            r_wheres.append("district ILIKE ?")
            r_params.append(f"%{district_filter}%")
        if utility_filter:
            r_wheres.append("utility ILIKE ?")
            r_params.append(f"%{utility_filter}%")
        r_where = "WHERE " + " AND ".join(r_wheres)

        sql_recent = f"""
            SELECT
                STRFTIME(CAST(scraped_at AS TIMESTAMP), '%Y-%m-%d') AS day,
                district,
                utility,
                COALESCE(SUM(CASE WHEN group_type='active'  THEN houses ELSE 0 END), 0) AS active_houses,
                COALESCE(SUM(CASE WHEN group_type='planned' THEN houses ELSE 0 END), 0) AS planned_houses,
                COALESCE(SUM(CASE WHEN group_type='active'  THEN 1 ELSE 0 END), 0) AS active_records,
                COALESCE(SUM(CASE WHEN group_type='planned' THEN 1 ELSE 0 END), 0) AS planned_records,
                COUNT(DISTINCT scraped_at) AS snapshots
            FROM power_outages
            {r_where}
            GROUP BY day, district, utility
        """
        cur = conn.execute(sql_recent, r_params)
        cols = [d[0] for d in cur.description]
        recent = [dict(zip(cols, row)) for row in cur.fetchall()]
        recent_keys = {(r["day"], r["district"], r["utility"]) for r in recent}

        # --- Archive: fill gaps from power_daily_archive ---
        a_wheres = ["day >= ?"]
        a_params: list = [cutoff]
        a_wheres.append("district != 'all'")
        if district_filter:
            a_wheres.append("district ILIKE ?")
            a_params.append(f"%{district_filter}%")
        if utility_filter:
            a_wheres.append("utility ILIKE ?")
            a_params.append(f"%{utility_filter}%")
        a_where = "WHERE " + " AND ".join(a_wheres)

        sql_archive = f"""
            SELECT day, district, utility,
                   active_houses, planned_houses,
                   active_records, planned_records, snapshots
            FROM power_daily_archive
            {a_where}
        """
        cur2 = conn.execute(sql_archive, a_params)
        cols2 = [d[0] for d in cur2.description]
        archive = [dict(zip(cols2, row)) for row in cur2.fetchall()]

        # Merge: prefer recent over archive
        for a in archive:
            key = (a["day"], a["district"], a["utility"])
            if key not in recent_keys:
                recent.append(a)

        recent.sort(key=lambda r: r["day"], reverse=True)
        return recent
    except Exception as e:
        log.error("query_power_history error: %s", e)
        return []
    finally:
        conn.close()


def query_power_history_by_day(
    district_filter: str | None = None,
    utility_filter: str | None = None,
    days: int = 30,
) -> list[dict]:
    """Агрегированная история по дням (суммируем по районам/типам).

    Возвращает: day, active_houses, planned_houses, active_records, planned_records
    """
    rows = query_power_history(district_filter, utility_filter, days)
    by_day: dict[str, dict] = {}
    for r in rows:
        d = r["day"]
        if d not in by_day:
            by_day[d] = {
                "day": d,
                "active_houses": 0, "planned_houses": 0,
                "active_records": 0, "planned_records": 0,
            }
        by_day[d]["active_houses"] += int(r.get("active_houses") or 0)
        by_day[d]["planned_houses"] += int(r.get("planned_houses") or 0)
        by_day[d]["active_records"] += int(r.get("active_records") or 0)
        by_day[d]["planned_records"] += int(r.get("planned_records") or 0)
    result = sorted(by_day.values(), key=lambda x: x["day"], reverse=True)
    return result


def query_power_history_by_district(
    utility_filter: str | None = None,
    days: int = 30,
) -> list[dict]:
    """Агрегированная история по районам (суммируем по дням).

    Возвращает: district, active_houses, planned_houses, days_with_outages
    """
    rows = query_power_history(utility_filter=utility_filter, days=days)
    by_dist: dict[str, dict] = {}
    for r in rows:
        dist = r["district"]
        if dist not in by_dist:
            by_dist[dist] = {
                "district": dist,
                "active_houses": 0, "planned_houses": 0,
                "_days": set(),
            }
        by_dist[dist]["active_houses"] += int(r.get("active_houses") or 0)
        by_dist[dist]["planned_houses"] += int(r.get("planned_houses") or 0)
        if int(r.get("active_houses") or 0) > 0:
            by_dist[dist]["_days"].add(r["day"])
    result = []
    for d in by_dist.values():
        result.append({
            "district": d["district"],
            "active_houses": d["active_houses"],
            "planned_houses": d["planned_houses"],
            "days_with_outages": len(d["_days"]),
        })
    result.sort(key=lambda x: x["active_houses"], reverse=True)
    return result


def query_power_efficiency(days: int = 30) -> list[dict]:
    """Оценка эффективности ремонтных бригад по районам.

    Алгоритм анализирует внутридневные паттерны из снимков power_outages:

    Хороший район:
      - Утреннее отключение (06-12) → устранено к вечеру (18+) → быстрая работа
      - Мало ночных (22-06) аварийных часов
      - Мало выходных аварий

    Плохой район:
      - Аварии сохраняются в вечернее (18-22) и ночное (22-06) время
      - Большое число домов × часов без ресурса
      - Частые выходные аварии

    Возвращает отсортированный по score (10 = отлично, 0 = плохо):
      district, score, grade, metrics{...}
    """
    init_power_table()
    conn = _get_conn()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        sql = """
            SELECT
                district,
                STRFTIME(CAST(scraped_at AS TIMESTAMP), '%%Y-%%m-%%d') AS day,
                EXTRACT(HOUR FROM CAST(scraped_at AS TIMESTAMP)) AS hour,
                DAYOFWEEK(CAST(scraped_at AS TIMESTAMP)) AS dow,
                SUM(houses) AS total_houses
            FROM power_outages
            WHERE scraped_at >= ?
              AND group_type = 'active'
              AND district != 'all'
              AND houses > 0
            GROUP BY district, day, hour, dow
            ORDER BY district, day, hour
        """
        cursor = conn.execute(sql, [cutoff])
        rows = cursor.fetchall()

        # Собираем данные по районам
        # dist → { day → { hour: houses, dow: int } }
        dist_data: dict[str, dict[str, dict]] = {}
        for district, day, hour, dow, houses in rows:
            if district not in dist_data:
                dist_data[district] = {}
            if day not in dist_data[district]:
                dist_data[district][day] = {"hours": {}, "dow": int(dow)}
            dist_data[district][day]["hours"][int(hour)] = int(houses)

        # Считаем метрики для каждого района
        results = []
        for district, day_map in dist_data.items():
            total_days = len(day_map)
            if total_days == 0:
                continue

            evening_days = 0     # дни с авариями в 18-22
            night_days = 0       # дни с авариями в 22-06
            weekend_days = 0     # выходные с авариями
            resolved_same_day = 0  # утром появилось — к вечеру устранено
            total_house_hours = 0
            evening_house_hours = 0
            peak_houses = 0

            for day, info in day_map.items():
                hours = info["hours"]
                dow = info["dow"]  # 0=Sun, 6=Sat

                # Максимум домов за день
                day_max = max(hours.values()) if hours else 0
                peak_houses = max(peak_houses, day_max)

                # Суммарная нагрузка (houses × snapshot_count ≈ house-hours)
                total_house_hours += sum(hours.values())

                # Утренние часы (06-12)
                morning = [h for h in hours if 6 <= h < 12]
                # Вечерние часы (18-22)
                evening = [h for h in hours if 18 <= h < 22]
                # Ночные часы (22-06)
                night = [h for h in hours if h >= 22 or h < 6]

                has_evening = len(evening) > 0
                has_night = len(night) > 0
                has_morning = len(morning) > 0

                if has_evening:
                    evening_days += 1
                    evening_house_hours += sum(hours[h] for h in evening)
                if has_night:
                    night_days += 1

                # Резолюция в тот же день: есть утром, нет вечером
                if has_morning and not has_evening and not has_night:
                    resolved_same_day += 1

                # Выходные (0=вс, 6=сб)
                if dow in (0, 6):
                    weekend_days += 1

            # ── Расчёт score (0-10) ──────────────────────────────────────
            score = 10.0

            # Штраф за вечерние аварии: чем больше дней с вечерними — тем хуже
            evening_ratio = evening_days / total_days if total_days > 0 else 0
            score -= evening_ratio * 3.0  # до -3

            # Штраф за ночные аварии (серьёзнее)
            night_ratio = night_days / total_days if total_days > 0 else 0
            score -= night_ratio * 4.0  # до -4

            # Штраф за выходные аварии
            weekend_ratio = weekend_days / total_days if total_days > 0 else 0
            score -= weekend_ratio * 1.5  # до -1.5

            # Бонус за быстрое устранение (утро → нет вечером)
            if total_days > 0:
                resolution_rate = resolved_same_day / total_days
                score += resolution_rate * 2.0  # до +2

            # Штраф за высокую нагрузку (house-hours)
            avg_house_hours = total_house_hours / total_days if total_days > 0 else 0
            if avg_house_hours > 200:
                score -= 1.5
            elif avg_house_hours > 100:
                score -= 1.0
            elif avg_house_hours > 50:
                score -= 0.5

            score = max(0.0, min(10.0, round(score, 1)))

            # Grade
            if score >= 8.0:
                grade = "A"
            elif score >= 6.0:
                grade = "B"
            elif score >= 4.0:
                grade = "C"
            elif score >= 2.0:
                grade = "D"
            else:
                grade = "F"

            results.append({
                "district": district,
                "score": score,
                "grade": grade,
                "metrics": {
                    "outage_days": total_days,
                    "evening_days": evening_days,
                    "night_days": night_days,
                    "weekend_days": weekend_days,
                    "resolved_same_day": resolved_same_day,
                    "resolution_rate": round(resolved_same_day / total_days * 100) if total_days > 0 else 0,
                    "total_house_hours": total_house_hours,
                    "evening_house_hours": evening_house_hours,
                    "peak_houses": peak_houses,
                },
            })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results
    except Exception as e:
        log.error("query_power_efficiency error: %s", e)
        return []
    finally:
        conn.close()
