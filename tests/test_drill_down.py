"""
══════════════════════════════════════════════════════════════════════════════
  ТЕСТЫ DRILL-DOWN (КОНТЕКСТНЫЕ ЦЕПОЧКИ ЗАПРОСОВ)
  Приоритет 1 · Критический пробел по отчёту QA
══════════════════════════════════════════════════════════════════════════════

Проверяем серверную часть drill-down:
- Извлечение района из запроса
- Стемминг районов (падежные формы)
- Роутинг уточняющих запросов
"""

import pytest


# ── ГРУППА 1: ИЗВЛЕЧЕНИЕ РАЙОНА ──────────────────────────────────────────────

class TestИзвлечениеРайона:
    """extract_district корректно извлекает район из разных форм запроса."""

    @pytest.mark.parametrize("query,expected", [
        ("школы в Центральном районе", "Центральный район"),
        ("аптеки в Советском районе", "Советский район"),
        ("стройки в Ленинском", "Ленинский район"),
        ("парки Кировского района", "Кировский район"),
        ("больницы Калининского района", "Калининский район"),
        ("остановки в Дзержинском районе", "Дзержинский район"),
        ("детсады Октябрьского района", "Октябрьский район"),
        ("спорт в Первомайском районе", "Первомайский район"),
        ("библиотеки Заельцовского района", "Заельцовский район"),
        ("культура Железнодорожного района", "Железнодорожный район"),
    ])
    def test_district_extraction(self, query, expected):
        from src.router import extract_district
        result = extract_district(query)
        assert result == expected, f"Для '{query}' ожидался '{expected}', получен '{result}'"


# ── ГРУППА 2: ПОДРАЙОНЫ ──────────────────────────────────────────────────────

class TestПодрайоны:
    """Подрайоны маршрутизируются к родительскому району."""

    @pytest.mark.parametrize("query,expected_parent", [
        ("школы в Академгородке", "Советский район"),
        ("аптеки на Шлюзе", "Советский район"),
        ("парки Верхней зоны", "Советский район"),
    ])
    def test_sub_district_to_parent(self, query, expected_parent):
        from src.router import extract_district
        result = extract_district(query)
        assert result == expected_parent


# ── ГРУППА 3: ЦЕПОЧКИ ЗАПРОСОВ (бэкенд) ─────────────────────────────────────

class TestЦепочкиЗапросов:
    """Последовательные запросы: город → район → уточнение."""

    def test_base_query_then_district(self):
        """Запрос 'школы' → затем 'школы в Советском районе' — оба работают."""
        from src.router import best_topic
        r1 = best_topic("школы")
        assert r1 is not None and r1.topic == "schools"

        r2 = best_topic("школы в Советском районе")
        assert r2 is not None and r2.topic == "schools"

    def test_district_filter_changes(self):
        """Переключение между районами — оба запроса маршрутизируются."""
        from src.router import best_topic
        r1 = best_topic("аптеки в Центральном районе")
        r2 = best_topic("аптеки в Ленинском районе")
        assert r1 is not None and r1.topic == "pharmacies"
        assert r2 is not None and r2.topic == "pharmacies"

    def test_count_after_filter(self):
        """'Сколько школ в Советском районе' → COUNT."""
        from src.router import best_topic
        r = best_topic("сколько школ в Советском районе")
        assert r is not None and r.topic == "schools"

    def test_top_n_with_district(self):
        """'Топ 5 аптек в Кировском районе' → TOP_N."""
        from src.router import best_topic
        r = best_topic("топ 5 аптек в Кировском районе")
        assert r is not None and r.topic == "pharmacies"


# ── ГРУППА 4: КОНТЕКСТ МЕЖДУ ЗАПРОСАМИ ───────────────────────────────────────

class TestСохранениеКонтекста:
    """Тема сохраняется между запросами (серверная часть)."""

    def test_same_topic_different_districts(self):
        """Несколько запросов одной темы в разных районах."""
        from src.router import best_topic
        queries = [
            "школы по районам",
            "школы в Центральном районе",
            "школы в Советском районе",
            "сколько школ",
        ]
        for q in queries:
            r = best_topic(q)
            assert r is not None and r.topic == "schools", f"'{q}' → {r}, ожидалось schools"
