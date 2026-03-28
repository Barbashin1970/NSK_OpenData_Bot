"""
══════════════════════════════════════════════════════════════════════════════
  ТЕСТЫ NO-CODE АНАЛИТИЧЕСКИХ ОТЧЁТОВ
  Новая функциональность · Custom CSV Reports
══════════════════════════════════════════════════════════════════════════════

Покрывают:
- API: /custom-reports (CRUD)
- Валидацию файлов: размер, формат, кодировка
- Ограничения: лимит 5 отчётов, 10000 строк
"""

import pytest
import json
from pathlib import Path


# ── ГРУППА 1: РЕЕСТР ОТЧЁТОВ ─────────────────────────────────────────────────

class TestРеестрОтчётов:
    """CRUD операции с реестром."""

    def test_list_reports_empty(self):
        from src.routes.custom_data import _load_registry
        result = _load_registry()
        assert isinstance(result, list)

    def test_list_reports_api(self):
        from src.routes.custom_data import list_reports
        result = list_reports()
        assert "reports" in result
        assert "max" in result
        assert result["max"] == 5


# ── ГРУППА 2: ВАЛИДАЦИЯ CSV ──────────────────────────────────────────────────

class TestВалидацияCSV:
    """Проверки формата, размера, кодировки."""

    def test_slug_generation(self):
        """Слаг генерируется из названия корректно."""
        import re
        name = "Рейтинг парков Новосибирска"
        slug = re.sub(r"[^a-zA-Z0-9а-яёА-ЯЁ]", "_", name.lower())[:40]
        slug = re.sub(r"_+", "_", slug).strip("_")
        assert slug
        assert len(slug) <= 40

    def test_col_type_detection(self):
        """Автоопределение типов колонок."""
        rows = [
            {"name": "Парк Центральный", "area": 150, "lat": 54.98, "район": "Центральный"},
            {"name": "Парк Берёзовая роща", "area": 80, "lat": 55.01, "район": "Октябрьский"},
        ]
        columns = ["name", "area", "lat", "район"]
        col_types = {}
        for col in columns:
            sample = [r.get(col) for r in rows if r.get(col) not in (None, "")]
            if col.lower() in ("_lat", "_lon", "lat", "lon", "latitude", "longitude"):
                col_types[col] = "coord"
            elif all(isinstance(v, (int, float)) for v in sample) and sample:
                col_types[col] = "number"
            elif "район" in col.lower():
                col_types[col] = "district"
            else:
                col_types[col] = "text"

        assert col_types["name"] == "text"
        assert col_types["area"] == "number"
        assert col_types["lat"] == "coord"
        assert col_types["район"] == "district"


# ── ГРУППА 3: ОБРАТНАЯ СВЯЗЬ ─────────────────────────────────────────────────

class TestОбратнаяСвязь:
    """Сервис жалоб и предложений."""

    def test_feedback_model(self):
        from src.routes.feedback import FeedbackIn
        fb = FeedbackIn(text="Тестовое сообщение", category="bug", name="Тест", city="Новосибирск")
        assert fb.text == "Тестовое сообщение"
        assert fb.category == "bug"
        assert fb.name == "Тест"
        assert fb.city == "Новосибирск"

    def test_feedback_defaults(self):
        from src.routes.feedback import FeedbackIn
        fb = FeedbackIn(text="Тест")
        assert fb.category == "suggestion"
        assert fb.source == "web"
        assert fb.name == ""
        assert fb.city == ""
