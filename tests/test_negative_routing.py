"""
══════════════════════════════════════════════════════════════════════════════
  НЕГАТИВНЫЕ ТЕСТЫ NLU-РОУТИНГА
  Приоритет 1 · Рекомендация QA: расширить покрытие негативных кейсов
══════════════════════════════════════════════════════════════════════════════

Проверяем что система корректно обрабатывает:
- Запросы вне контекста городских данных
- Бессмысленные наборы слов
- SQL-инъекции и XSS
- Очень длинные запросы
- Запросы с эмодзи
- Запросы на других языках
"""

import pytest


# ── ГРУППА 1: ЗАПРОСЫ ВНЕ КОНТЕКСТА ──────────────────────────────────────────

class TestОтсутствиеЛожныхСрабатываний:
    """Запросы не связанные с городскими данными → topic=None или UNKNOWN."""

    @pytest.mark.parametrize("query", [
        "рецепт борща",
        "кто президент России",
        "напиши стихотворение",
        "сколько будет 2+2",
        "переведи на английский",
        "расскажи анекдот",
        "что происходит на Марсе",
        "как приготовить пиццу",
        "реферат по истории",
        "лучший фильм 2025 года",
    ])
    def test_offtopic_returns_none(self, query):
        from src.router import best_topic
        result = best_topic(query)
        assert result is None or result.topic == "unknown", \
            f"Запрос '{query}' не должен маршрутизироваться к теме, но получил {result}"


# ── ГРУППА 2: БЕССМЫСЛЕННЫЕ ЗАПРОСЫ ──────────────────────────────────────────

class TestБессмысленныеЗапросы:
    """Случайные наборы слов, пустые строки, спецсимволы."""

    @pytest.mark.parametrize("query", [
        "",
        "   ",
        "абвгдежз",
        "123456789",
        "!@#$%^&*()",
        "...---...",
        "а а а а а а а",
        "null undefined NaN",
    ])
    def test_garbage_input_no_crash(self, query):
        from src.router import best_topic
        # Не должно быть исключения
        result = best_topic(query)
        # Допускаем None или UNKNOWN
        assert result is None or hasattr(result, 'topic')


# ── ГРУППА 3: БЕЗОПАСНОСТЬ — SQL-ИНЪЕКЦИИ И XSS ─────────────────────────────

class TestБезопасностьВвода:
    """SQL-инъекции и XSS в поисковых запросах не ломают систему."""

    @pytest.mark.parametrize("query", [
        "'; DROP TABLE power_outages; --",
        "школы\" OR 1=1 --",
        "<script>alert('xss')</script>",
        "<img src=x onerror=alert(1)>",
        "аптеки UNION SELECT * FROM users",
        "школы; DELETE FROM cache;",
        "' AND '1'='1",
    ])
    def test_injection_safe(self, query):
        from src.router import best_topic
        # Не должно быть ошибки — система просто не найдёт тему
        result = best_topic(query)
        # Может вернуть None или topic (если слово "школы"/"аптеки" распознано)
        assert result is None or hasattr(result, 'topic')


# ── ГРУППА 4: ДЛИННЫЕ ЗАПРОСЫ ────────────────────────────────────────────────

class TestДлинныеЗапросы:
    """Очень длинные запросы не вызывают падение или таймаут."""

    def test_very_long_query_500_chars(self):
        from src.router import best_topic
        query = "школы " * 100  # ~600 символов
        result = best_topic(query)
        # Должен распознать тему "schools" даже в длинном запросе
        assert result is not None
        assert result.topic == "schools"

    def test_extremely_long_query_5000_chars(self):
        from src.router import best_topic
        query = "а" * 5000
        result = best_topic(query)
        assert result is None or hasattr(result, 'topic')


# ── ГРУППА 5: ЗАПРОСЫ С ЭМОДЗИ ──────────────────────────────────────────────

class TestЗапросыСЭмодзи:
    """Запросы содержащие эмодзи обрабатываются корректно."""

    @pytest.mark.parametrize("query,expected_topic", [
        ("🏫 школы в городе", "schools"),
        ("💊 аптеки рядом", "pharmacies"),
        ("🌿 воздух в городе 🌍", "ecology"),
        ("⚡ отключения электричества 💡", "power_outages"),
    ])
    def test_emoji_in_query(self, query, expected_topic):
        from src.router import best_topic
        result = best_topic(query)
        assert result is not None, f"Запрос с эмодзи '{query}' не распознан"
        assert result.topic == expected_topic


# ── ГРУППА 6: ЗАПРОСЫ НА ДРУГИХ ЯЗЫКАХ ───────────────────────────────────────

class TestДругиеЯзыки:
    """Запросы на иностранных языках — graceful fallback."""

    @pytest.mark.parametrize("query", [
        "show me all schools",
        "where are the pharmacies",
        "Zeige mir die Schulen",
        "显示学校",
        "学校を見せて",
    ])
    def test_foreign_language_no_crash(self, query):
        from src.router import best_topic
        result = best_topic(query)
        # Может вернуть None или тему — главное не падает
        assert result is None or hasattr(result, 'topic')
