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
from .rule_engine import rules as _rules

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
    """Агрегированная история по дням.

    Для каждого дня возвращает СРЕДНЕЕ кол-во домов в моменте (без снежного кома):
      Шаг 1: для каждой (район × utility) считаем avg = SUM(houses) / snapshots.
      Шаг 2: суммируем эти avg по всем (район × utility) дня.
    """
    rows = query_power_history(district_filter, utility_filter, days)
    by_day: dict[str, dict] = {}
    for r in rows:
        d = r["day"]
        if d not in by_day:
            by_day[d] = {
                "day": d,
                "active_houses": 0.0, "planned_houses": 0.0,
                "active_records": 0, "planned_records": 0,
            }
        snaps = int(r.get("snapshots") or 0)
        if snaps > 0:
            # Среднее в моменте для этой (район × utility) пары
            by_day[d]["active_houses"] += int(r.get("active_houses") or 0) / snaps
            by_day[d]["planned_houses"] += int(r.get("planned_houses") or 0) / snaps
        by_day[d]["active_records"] += int(r.get("active_records") or 0)
        by_day[d]["planned_records"] += int(r.get("planned_records") or 0)
    result = []
    for d in by_day.values():
        d["active_houses"] = int(round(d["active_houses"]))
        d["planned_houses"] = int(round(d["planned_houses"]))
        result.append(d)
    result.sort(key=lambda x: x["day"], reverse=True)
    return result


def query_power_history_by_district(
    utility_filter: str | None = None,
    days: int = 30,
) -> list[dict]:
    """Агрегированная история по районам.

    Возвращает на каждый район:
      - active_houses, planned_houses — СУММА по снимкам (legacy, "снежный ком")
      - planned_avg_daily — среднее число домов в плане в любой момент
        (рассчитано как SUM(planned)/SUM(snapshots), усреднено по периоду)
      - planned_now — текущее число домов в плане (последний снимок per utility)
      - days_with_outages — кол-во дней с активными авариями (rounded avg > 0)
      - days_with_any_outage — кол-во дней с любыми отключениями (active OR planned)
        ВАЖНО: используется как (30 - days_with_any) для "чистых дней" в рейтинге,
        иначе расходится с per-day отчётом, где день "чистый" iff active=0 AND planned=0
    """
    rows = query_power_history(utility_filter=utility_filter, days=days)
    # Дни, за которые сборщик отработал хоть по одному району
    days_observed_all = len({r["day"] for r in rows if r.get("day")})
    by_dist: dict[str, dict] = {}
    # (district, day) → {active_avg, planned_avg} — повторяет логику by_day,
    # чтобы day-level "чистый" определялся одинаково в обоих представлениях
    by_dist_day: dict[tuple, dict] = {}

    for r in rows:
        dist = r["district"]
        if dist not in by_dist:
            by_dist[dist] = {
                "district": dist,
                "_active_sum_avg": 0.0,
                "_planned_sum_avg": 0.0,
                "_days_seen": set(),
            }
        snaps = int(r.get("snapshots") or 0)
        if snaps > 0:
            by_dist[dist]["_active_sum_avg"] += int(r.get("active_houses") or 0) / snaps
            by_dist[dist]["_planned_sum_avg"] += int(r.get("planned_houses") or 0) / snaps
        by_dist[dist]["_days_seen"].add(r["day"])

        # Day-level накопление (для clean_days, как в by_day)
        key = (dist, r["day"])
        if key not in by_dist_day:
            by_dist_day[key] = {"active_avg": 0.0, "planned_avg": 0.0}
        if snaps > 0:
            by_dist_day[key]["active_avg"] += int(r.get("active_houses") or 0) / snaps
            by_dist_day[key]["planned_avg"] += int(r.get("planned_houses") or 0) / snaps

    # Считаем "дни с любыми отключениями" по той же логике, что и frontend chart:
    # день считается "не-чистым", если rounded avg active > 0 ИЛИ rounded avg planned > 0
    days_active_by_dist: dict[str, set] = {}
    days_any_by_dist: dict[str, set] = {}
    for (dist, day), v in by_dist_day.items():
        a = int(round(v["active_avg"]))
        p = int(round(v["planned_avg"]))
        if a > 0:
            days_active_by_dist.setdefault(dist, set()).add(day)
        if a > 0 or p > 0:
            days_any_by_dist.setdefault(dist, set()).add(day)

    # "Сейчас": последний снимок per district из power_outages
    now_active = _query_now_by_district(group_type="active", utility_filter=utility_filter)
    now_planned = _query_now_by_district(group_type="planned", utility_filter=utility_filter)

    result = []
    for d in by_dist.values():
        days_seen = max(len(d["_days_seen"]), 1)
        avg_active = int(round(d["_active_sum_avg"] / days_seen))
        avg_planned = int(round(d["_planned_sum_avg"] / days_seen))
        dist_name = d["district"]
        result.append({
            "district": dist_name,
            "active_houses": now_active.get(dist_name, 0),
            "planned_houses": now_planned.get(dist_name, 0),
            "active_avg_daily": avg_active,
            "planned_avg_daily": avg_planned,
            "active_now": now_active.get(dist_name, 0),
            "planned_now": now_planned.get(dist_name, 0),
            "days_with_outages": len(days_active_by_dist.get(dist_name, set())),
            "days_with_any_outage": len(days_any_by_dist.get(dist_name, set())),
            # Знаменатель для «чистых дней» на фронте — общегородской: день,
            # в который у района нет строк, означает отсутствие отключений,
            # а не отсутствие наблюдения (см. _query_observed_days).
            "days_observed": days_observed_all,
        })
    result.sort(key=lambda x: x["active_now"], reverse=True)
    return result


def _query_now_by_district(
    group_type: str = "active",
    utility_filter: str | None = None,
) -> dict[str, int]:
    """Возвращает {district: houses} из самого свежего снимка power_outages.

    group_type='active' — сейчас аварийных
    group_type='planned' — сейчас плановых
    """
    init_power_table()
    conn = _get_conn()
    try:
        wheres = ["group_type = ?", "district != 'all'"]
        params: list = [group_type]
        if utility_filter:
            wheres.append("utility ILIKE ?")
            params.append(f"%{utility_filter}%")
        where = "WHERE " + " AND ".join(wheres)
        sql = f"""
            WITH latest AS (
                SELECT MAX(scraped_at) AS ts
                FROM power_outages
                {where}
            )
            SELECT district, SUM(houses) AS h
            FROM power_outages
            {where}
              AND scraped_at = (SELECT ts FROM latest)
            GROUP BY district
        """
        cur = conn.execute(sql, params + params)
        return {row[0]: int(row[1] or 0) for row in cur.fetchall()}
    except Exception as e:
        log.debug("_query_now_by_district(%s): %s", group_type, e)
        return {}
    finally:
        try: conn.close()
        except: pass


# Backward-compatible alias (старый код может ссылаться)
def _query_planned_now_by_district(utility_filter: str | None = None) -> dict[str, int]:
    return _query_now_by_district("planned", utility_filter)


# Минимум дней с почасовыми снимками, при котором внутридневные метрики
# (вечерние/ночные аварии, устранение за день) считаются показательными.
_MIN_HOUR_DAYS = 5


def _ramp(value: float, bad: float, good: float) -> float:
    """Нормирует метрику в 0..1 между якорями «плохо» и «хорошо».

    0.0 при value = bad, 1.0 при value = good, линейно между, срез по краям.
    Направление задают сами якоря: для метрик «чем меньше, тем лучше»
    (число домов без ресурса) bad больше good.
    """
    bad, good = float(bad), float(good)
    if good == bad:
        return 0.0
    return max(0.0, min(1.0, (float(value) - bad) / (good - bad)))


def _query_observed_days(conn, cutoff_iso: str) -> set:
    """Множество дней периода, за которые сборщик вообще отработал.

    Знаменатель для «чистых дней» — общегородской, а не по району. Строка в
    архив пишется только при наличии отключения на портале, поэтому день без
    строк по конкретному району означает, что у района отключений не было, а
    вовсе не отсутствие данных. Считать наблюдённые дни по району = объявить
    благополучные районы ненаблюдавшимися и обнулить им бонус.

    А вот дни, когда данных нет НИ ПО ОДНОМУ району, — это простой сборщика
    (в ряду есть многодневные провалы), и вот их из знаменателя надо убрать,
    иначе авария сборщика засчитается городу как период без отключений.
    """
    observed: set = set()
    day_cutoff = cutoff_iso[:10]
    try:
        cur = conn.execute(
            "SELECT DISTINCT day FROM power_daily_archive WHERE day >= ? AND district != 'all'",
            [day_cutoff],
        )
        observed.update(r[0] for r in cur.fetchall())
    except Exception as e:
        log.debug("_query_observed_days: архив недоступен (%s)", e)
    try:
        cur = conn.execute(
            """
            SELECT DISTINCT STRFTIME(CAST(scraped_at AS TIMESTAMP), '%Y-%m-%d')
            FROM power_outages WHERE scraped_at >= ?
            """,
            [cutoff_iso],
        )
        observed.update(r[0] for r in cur.fetchall())
    except Exception as e:
        log.debug("_query_observed_days: raw недоступен (%s)", e)
    return observed


def query_observed_days(days: int = 30) -> list[str]:
    """Отсортированный список дней периода, за которые сборщик отработал.

    Нужен фронту, чтобы отличить «в этот день отключений не было» (строк нет,
    но день наблюдался) от «в этот день сбор не работал» (провал в ряду).
    Первое — чистый день, второе — дырка, и красить их одинаково нельзя.
    """
    init_power_table()
    conn = _get_conn()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        return sorted(_query_observed_days(conn, cutoff))
    except Exception as e:
        log.error("query_observed_days error: %s", e)
        return []
    finally:
        conn.close()


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
        # Двухступенчатая агрегация (устраняет "снежный ком"):
        # 1) В одном snapshot суммируем дома по utility-типам
        # 2) Внутри часа берём MAX по снимкам (а не SUM — снимки повторяют состояние)
        sql = """
            SELECT
                district, day, hour, dow,
                MAX(snap_houses) AS total_houses
            FROM (
                SELECT
                    district,
                    STRFTIME(CAST(scraped_at AS TIMESTAMP), '%Y-%m-%d') AS day,
                    EXTRACT(HOUR FROM CAST(scraped_at AS TIMESTAMP)) AS hour,
                    DAYOFWEEK(CAST(scraped_at AS TIMESTAMP)) AS dow,
                    scraped_at,
                    SUM(houses) AS snap_houses
                FROM power_outages
                WHERE scraped_at >= ?
                  AND group_type = 'active'
                  AND district != 'all'
                  AND houses > 0
                GROUP BY district, day, hour, dow, scraped_at
            ) t
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

        # ── Дополнение из архива ─────────────────────────────────────────────
        # power_outages хранит только последние снимки (TTL ~7-10 дней),
        # старые дни уже в power_daily_archive. Без этого clean_days считается
        # неверно (был баг "29 чистых" у всех районов).
        archive_cutoff = cutoff[:10]  # YYYY-MM-DD
        archive_sql = """
            SELECT district, day, active_houses
            FROM power_daily_archive
            WHERE day >= ?
              AND active_houses > 0
              AND district != 'all'
        """
        try:
            arch_cursor = conn.execute(archive_sql, [archive_cutoff])
            for district, day, _ah in arch_cursor.fetchall():
                if district not in dist_data:
                    dist_data[district] = {}
                # Если день уже из raw — оставляем (он точнее по часам).
                # Если только из архива — помечаем как "архивный": hours={} но день учтён.
                if day not in dist_data[district]:
                    # dow считаем из даты: DuckDB DAYOFWEEK даёт 0=вс…6=сб,
                    # python weekday() — 0=пн…6=вс. Без пересчёта архивные дни
                    # получали dow=0 (воскресенье) и все шли в «выходные».
                    try:
                        _wd = datetime.strptime(day, "%Y-%m-%d").weekday()
                        _dow = (_wd + 1) % 7
                    except ValueError:
                        _dow = -1
                    dist_data[district][day] = {"hours": {}, "dow": _dow, "from_archive": True}
        except Exception as e:
            log.debug("query_power_efficiency: архив пропущен (%s)", e)

        # ── Дни наблюдения по районам ────────────────────────────────────────
        # ВАЖНО: сбор данных прерывался (см. пропуски в power_daily_archive),
        # поэтому «чистые дни» нельзя считать как (календарный период - дни с
        # авариями) — так пропуски сборщика превращаются в чистые дни.
        # Знаменатель = число дней, за которые у района вообще есть данные.
        observed = _query_observed_days(conn, cutoff)

        # Среднесуточное число аварийных домов по районам — считается из
        # суточного архива и потому доступно на всю глубину, в отличие от
        # почасовых снимков. Нужно, чтобы на длинном периоде было чем
        # различать районы: без внутридневных метрик все получают базовый балл.
        avg_houses: dict[str, int] = {}
        try:
            for r in query_power_history_by_district(days=days):
                avg_houses[r["district"]] = int(r.get("active_avg_daily") or 0)
        except Exception as e:
            log.debug("query_power_efficiency: суточная нагрузка недоступна (%s)", e)

        # Считаем метрики для каждого района
        results = []
        for district, day_map in dist_data.items():
            total_days = len(day_map)
            if total_days == 0:
                continue

            # Дни с почасовыми снимками — только по ним считаются внутридневные
            # метрики (вечер/ночь/устранение). Архивные дни знают лишь суточный
            # итог, включать их в знаменатель — занижать долю и завышать балл.
            hour_days = sum(1 for info in day_map.values() if info["hours"])
            hour_denom = max(hour_days, 1)
            # На выборке в 1-2 дня доля «вечерних аварий» скачет между 0 и 1 и
            # определяет грейд сильнее, чем реальная работа бригад. Ниже порога
            # внутридневные компоненты не начисляем вовсе — балл считается по
            # доле чистых дней, а неполнота честно видна в hour_days.
            intraday_ok = hour_days >= _MIN_HOUR_DAYS

            evening_days = 0     # дни с авариями в 18-22
            night_days = 0       # дни с авариями в 22-06
            weekend_days = 0     # выходные с авариями
            resolved_same_day = 0  # утром появилось — к вечеру устранено
            total_house_hours = 0
            evening_house_hours = 0
            night_house_hours = 0
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
                    night_house_hours += sum(hours[h] for h in night)

                # Резолюция в тот же день: есть утром, нет вечером
                if has_morning and not has_evening and not has_night:
                    resolved_same_day += 1

                # Выходные (0=вс, 6=сб)
                if dow in (0, 6):
                    weekend_days += 1

            # ── Расчёт score (0-10) из YAML-регламента ──────────────────
            # Накопительная модель: балл собирается из компонентов, а не
            # вычитается из десятки. В прежней версии штрафы были мелкими,
            # все районы упирались в потолок и получали один грейд, хотя
            # доля дней без аварий различалась втрое.
            cfg = _rules.get("power_rating_rules")
            gen = cfg.get("general", {})
            sc = cfg.get("scoring", {})

            days_observed = len(observed) or total_days
            clean_days = max(0, days_observed - total_days)
            clean_ratio = clean_days / days_observed if days_observed > 0 else 0
            dist_avg_houses = avg_houses.get(district, 0)
            avg_house_hours = total_house_hours / hour_denom if intraday_ok else 0

            rel_cfg = sc.get("reliability", {})
            load_cfg = sc.get("load", {})
            intra_cfg = sc.get("intraday", {})
            w_rel = float(rel_cfg.get("weight", 4.0))
            w_load = float(load_cfg.get("weight", 4.0))
            w_intra = float(intra_cfg.get("weight", 2.0))

            n_rel = _ramp(clean_ratio, rel_cfg.get("bad", 0.05), rel_cfg.get("good", 0.55))
            n_load = _ramp(dist_avg_houses, load_cfg.get("bad", 200), load_cfg.get("good", 10))

            # Внутридневные метрики — только по дням с почасовыми снимками.
            # Доли считаем от аварийной НАГРУЗКИ (домо-часов), а не по признаку
            # «была ли авария»: в миллионнике ночью почти всегда что-то
            # отключено, и бинарный признак районы не различает вовсе.
            parts = intra_cfg.get("parts", {})
            evening_share = evening_house_hours / total_house_hours if total_house_hours else 0.0
            night_share = night_house_hours / total_house_hours if total_house_hours else 0.0
            weekend_ratio = weekend_days / total_days if total_days > 0 else 0.0

            if intraday_ok:
                p_night = parts.get("night_share", {})
                p_even = parts.get("evening_share", {})
                p_wend = parts.get("weekend_share", {})
                n_intra = (
                    _ramp(night_share, p_night.get("bad", 0.45), p_night.get("good", 0.18))
                    + _ramp(evening_share, p_even.get("bad", 0.25), p_even.get("good", 0.04))
                    + _ramp(weekend_ratio, p_wend.get("bad", 0.45), p_wend.get("good", 0.18))
                ) / 3
            else:
                n_intra = None

            if n_intra is None and cfg.get("fallback", {}).get("redistribute_intraday", True):
                # Нет почасовых данных — вес intraday распределяем на остальные
                # компоненты, чтобы шкала осталась 0-10 и грейды не съехали
                total_w = w_rel + w_load
                if total_w > 0:
                    k = (w_rel + w_load + w_intra) / total_w
                    w_rel, w_load = w_rel * k, w_load * k
                w_intra, n_intra = 0.0, 0.0

            score = w_rel * n_rel + w_load * n_load + w_intra * (n_intra or 0.0)
            score = max(
                float(gen.get("min_score", 0.0)),
                min(float(gen.get("max_score", 10.0)), round(score, 1)),
            )
            resolution_rate = resolved_same_day / hour_denom if intraday_ok else 0.0

            # Grade из YAML
            grade = "F"
            for g in cfg.get("grades", []):
                if score >= float(g.get("threshold", 0)):
                    grade = g["grade"]
                    break

            results.append({
                "district": district,
                "score": score,
                "grade": grade,
                "metrics": {
                    "outage_days": total_days,
                    "clean_days": clean_days,
                    "days_observed": days_observed,
                    "hour_days": hour_days,
                    "intraday_metrics": intraday_ok,
                    "active_avg_daily": dist_avg_houses,
                    "evening_days": evening_days,
                    "night_days": night_days,
                    "evening_share": round(
                        evening_house_hours / total_house_hours, 3
                    ) if total_house_hours else 0,
                    "night_share": round(
                        night_house_hours / total_house_hours, 3
                    ) if total_house_hours else 0,
                    "weekend_days": weekend_days,
                    "resolved_same_day": resolved_same_day,
                    "resolution_rate": round(resolution_rate * 100),
                    "total_house_hours": total_house_hours,
                    "evening_house_hours": evening_house_hours,
                    "avg_house_hours": round(avg_house_hours),
                    "peak_houses": peak_houses,
                    # Разложение балла — чтобы в UI было видно, где район теряет
                    "components": {
                        "reliability": round(w_rel * n_rel, 2),
                        "load": round(w_load * n_load, 2),
                        "intraday": round(w_intra * (n_intra or 0.0), 2),
                        "max": {
                            "reliability": round(w_rel, 2),
                            "load": round(w_load, 2),
                            "intraday": round(w_intra, 2),
                        },
                    },
                },
            })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results
    except Exception as e:
        log.error("query_power_efficiency error: %s", e)
        return []
    finally:
        conn.close()
