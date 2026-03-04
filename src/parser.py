"""Чтение и нормализация CSV-файлов.

Поддерживает кодировки utf-8-sig и cp1251, разделители , и ;.
Переименовывает ключевые колонки в стандартные (_district, _street, _name).
"""

import csv
import io
import logging
import sys
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Увеличиваем лимит размера поля CSV (по умолчанию 131072, некоторые поля крупнее)
csv.field_size_limit(min(sys.maxsize, 10 * 1024 * 1024))

ENCODINGS = ["utf-8-sig", "utf-8", "cp1251", "cp866"]
DELIMITERS = [",", ";", "\t"]


def _decode(raw: bytes) -> tuple[str, str]:
    """Определяет кодировку и возвращает (text, encoding)."""
    for enc in ENCODINGS:
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace"), "utf-8-replace"


def _detect_delimiter(sample: str) -> str:
    """Определяет разделитель по первой строке."""
    first_line = sample.split("\n")[0]
    counts = {d: first_line.count(d) for d in DELIMITERS}
    return max(counts, key=counts.get)


def read_csv(file_path: Path, dataset_config: dict[str, Any]) -> list[dict[str, str]]:
    """Читает CSV и возвращает список строк как dict.

    Добавляет служебные поля:
      _district — район
      _street   — улица
      _name     — наименование объекта
    """
    raw = file_path.read_bytes()
    text, enc = _decode(raw)
    delimiter = _detect_delimiter(text)

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

    fields = dataset_config.get("fields", {})
    district_col = fields.get("district_col", "")
    street_col = fields.get("street_col", "")
    name_col = fields.get("name_col", "")

    rows = []
    for i, row in enumerate(reader):
        # Убираем лишние пробелы и кавычки из ключей; пропускаем None-ключи (trailing comma)
        cleaned = {
            k.strip().strip('"'): (v or "").strip().strip('"')
            for k, v in row.items()
            if k is not None and k.strip()
        }

        # Добавляем стандартизированные поля
        cleaned["_district"] = cleaned.get(district_col, "").strip()
        cleaned["_street"] = cleaned.get(street_col, "").strip()
        cleaned["_name"] = cleaned.get(name_col, "").strip()

        rows.append(cleaned)

    log.info(f"Прочитано {len(rows)} строк из {file_path.name} (enc={enc}, sep='{delimiter}')")
    return rows


def get_columns(file_path: Path) -> list[str]:
    """Возвращает список колонок CSV."""
    raw = file_path.read_bytes()
    text, _ = _decode(raw)
    delimiter = _detect_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    return [k.strip().strip('"') for k in (reader.fieldnames or [])]
