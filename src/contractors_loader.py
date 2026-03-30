"""Загрузчик справочника контрагентов из CSV и open data.

Импортирует аварийные/диспетчерские службы Новосибирска из CSV
и строительные компании из датасетов opendata при первом запуске.
"""

import csv
import logging
from pathlib import Path

from .task_store import init_task_tables, upsert_contractor, contractors_count, get_contractors, update_contractor

log = logging.getLogger(__name__)

_CSV_PATH = Path(__file__).parent.parent / "data" / "contractors_nsk.csv"

# Маппинг колонок CSV → поля контрагента
_CSV_MAP = {
    "Категория_службы":          "category",
    "Наименование_организации":  "org_name",
    "Телефон_дежурной_службы":   "duty_phone",
    "Режим_работы":              "work_hours",
    "ФИО_руководителя":          "head_name",
    "Телефон_руководителя":      "head_phone",
}


def seed_contractors() -> int:
    """Загружает контрагентов из CSV, если таблица пуста.

    Возвращает количество загруженных записей (0 если уже загружены).
    """
    init_task_tables()

    if contractors_count() > 0:
        log.info("Task Space: контрагенты уже загружены (%d)", contractors_count())
        return 0

    if not _CSV_PATH.exists():
        log.warning("Task Space: файл %s не найден, пропускаю загрузку", _CSV_PATH)
        return 0

    count = 0
    with open(_CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapped = {}
            for csv_col, field in _CSV_MAP.items():
                mapped[field] = row.get(csv_col, "").strip()
            upsert_contractor(mapped)
            count += 1

    log.info("Task Space: загружено %d контрагентов из CSV", count)
    return count


_CONSTRUCTION_CATEGORY = "Строительная компания"


def seed_construction_contractors() -> int:
    """Загружает строительные компании из датасета активных строек.

    Для каждого уникального застройщика создаёт карточку контрагента,
    в поле comment собирает объекты с районами. Не дублирует существующих.
    Возвращает количество созданных записей.
    """
    init_task_tables()

    # Проверяем, есть ли уже строительные компании
    existing = get_contractors(category=_CONSTRUCTION_CATEGORY)
    if existing:
        log.info("Task Space: строительные компании уже загружены (%d)", len(existing))
        return 0

    try:
        from .construction_opendata import query_active, permits_available
    except ImportError:
        log.warning("Task Space: модуль construction_opendata недоступен")
        return 0

    if not permits_available():
        log.warning("Task Space: данные о стройках не загружены, пропускаю")
        return 0

    # Собираем все активные стройки
    rows, total = query_active(limit=3000)
    if not rows:
        return 0

    # Группируем объекты по застройщику
    builders: dict[str, list[str]] = {}
    for r in rows:
        name = (r.get("Zastr") or "").strip()
        if not name:
            continue
        # Фильтруем только юр.лица (ООО, АО, МУП, ЗАО, ПАО, ГУП)
        name_up = name.upper()
        if not any(t in name_up for t in ("ООО", "АО ", "МУП", "ЗАО", "ПАО", "ГУП")):
            continue
        obj_name = (r.get("NameOb") or "").strip()[:80]
        district = (r.get("district") or "").strip()
        entry = obj_name
        if district:
            entry = f"{obj_name} ({district})"
        if name not in builders:
            builders[name] = []
        # Не дублируем одинаковые объекты
        if entry not in builders[name]:
            builders[name].append(entry)

    count = 0
    for org_name, objects in builders.items():
        comment_lines = [f"Активных объектов: {len(objects)}"]
        # Показываем до 5 объектов в комментарии
        for obj in objects[:5]:
            comment_lines.append(f"• {obj}")
        if len(objects) > 5:
            comment_lines.append(f"... и ещё {len(objects) - 5}")

        upsert_contractor({
            "category": _CONSTRUCTION_CATEGORY,
            "org_name": org_name,
            "comment": "\n".join(comment_lines),
        })
        count += 1

    log.info("Task Space: загружено %d строительных компаний из opendata", count)
    return count
