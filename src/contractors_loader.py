"""Загрузчик справочника контрагентов из CSV.

Импортирует аварийные и диспетчерские службы Новосибирска
в таблицу ts_contractors при первом запуске.
"""

import csv
import logging
from pathlib import Path

from .task_store import init_task_tables, upsert_contractor, contractors_count

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
