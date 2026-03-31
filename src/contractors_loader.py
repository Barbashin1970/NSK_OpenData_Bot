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


_EMERGENCY_CATEGORY = "Служба ГО и ЧС"

_EMERGENCY_DATA = [
    ("Отдел по делам ГО и ЧС по Советскому району", "+7 (383) 333-22-12", "Советский", "Академика Лаврентьева проспект, 14"),
    ("Аварийно-спасательный отряд (АСО) «Южный»", "112", "Советский", "Бердское шоссе, 302"),
    ("Отдел по делам ГО и ЧС по Калининскому району", "+7 (383) 204-28-55", "Калининский", "Богдана Хмельницкого улица, 50"),
    ("Отдел по делам ГО и ЧС по Дзержинскому району", "+7 (383) 218-69-77", "Дзержинский", "Дзержинского проспект, 16"),
    ("МКУ «Единая дежурно-диспетчерская служба г. Новосибирска»", "+7 (383) 218-00-51", "Центральный", "Колыванская улица, 4"),
    ("МКУ «Служба аварийно-спасательных работ и гражданской защиты»", "+7 (383) 218-68-00", "Центральный", "Колыванская улица, 4"),
    ("Аварийно-спасательный отряд (АСО) «Центральный»", "112", "Центральный", "Колыванская улица, 4"),
    ("Отдел по делам ГО и ЧС по Заельцовскому, Железнодорожному и Центральному районам", "+7 (383) 227-58-58", "Железнодорожный", "Ленина улица, 57"),
    ("Аварийно-спасательный отряд (АСО) «Западный»", "112", "Кировский", "Обской переулок, 41"),
    ("Отдел по делам ГО и ЧС по Кировскому району", "+7 (383) 227-48-26", "Кировский", "Петухова улица, 18"),
    ("Курсы гражданской обороны", "+7 (383) 279-40-28", "Дзержинский", "Промышленная улица, 1а"),
    ("Департамент по ЧС и взаимодействию с адм. органами мэрии", "+7 (383) 229-67-00", "Центральный", "Романова улица, 33"),
    ("Управление мэрии по делам ГО, ЧС и пожарной безопасности", "+7 (383) 229-67-44", "Центральный", "Романова улица, 33"),
    ("Отдел по делам ГО и ЧС по Октябрьскому району", "+7 (383) 228-81-99", "Октябрьский", "Сакко и Ванцетти улица, 33"),
    ("Аварийно-спасательный отряд (АСО) «Приморский»", "112", "Советский", "Софийская улица, 15"),
    ("Отдел по делам ГО и ЧС по Ленинскому району", "+7 (383) 361-00-41", "Ленинский", "Станиславского улица, 6а"),
    ("Отдел по делам ГО и ЧС по Первомайскому району", "+7 (383) 228-85-54", "Первомайский", "Физкультурная улица, 7"),
]


def seed_emergency_contractors() -> int:
    """Загружает службы ГО и ЧС по районам, если ещё не загружены."""
    init_task_tables()
    existing = get_contractors(category=_EMERGENCY_CATEGORY)
    if existing:
        log.info("Task Space: службы ГО и ЧС уже загружены (%d)", len(existing))
        return 0

    count = 0
    for org_name, phone, district, address in _EMERGENCY_DATA:
        upsert_contractor({
            "category": _EMERGENCY_CATEGORY,
            "org_name": org_name,
            "duty_phone": phone,
            "work_hours": "Круглосуточно",
            "address": address,
            "district": district,
        })
        count += 1

    log.info("Task Space: загружено %d служб ГО и ЧС", count)
    return count
