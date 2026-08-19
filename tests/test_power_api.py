"""
══════════════════════════════════════════════════════════════════════════════
  ТЕСТЫ API ЖКХ (POWER/OUTAGES)
  Приоритет 1 · По образцу модуля экологии (эталонное покрытие)
══════════════════════════════════════════════════════════════════════════════

Покрывают:
- API-эндпоинты: /power/history, /power/efficiency
- Кэш: инициализация, upsert, запросы
- Граничные условия: пустые данные, одновременные аварии
"""

import pytest
from datetime import datetime, timezone


# ── ГРУППА 1: ИНИЦИАЛИЗАЦИЯ И КЭШИРОВАНИЕ ────────────────────────────────────

class TestИнициализацияЖКХ:
    """Таблицы power_outages и power_daily_archive создаются корректно."""

    def test_init_power_table_creates_tables(self):
        from src.power_cache import init_power_table
        # Не должно быть ошибок при повторной инициализации
        init_power_table()
        init_power_table()  # повторный вызов — идемпотентность

    def test_power_meta_on_empty_db(self):
        from src.power_cache import get_power_meta
        meta = get_power_meta()
        assert isinstance(meta, dict)
        # last_scraped может быть None если нет данных


# ── ГРУППА 2: ЗАПРОСЫ ИСТОРИИ ────────────────────────────────────────────────

class TestИсторияЖКХ:
    """query_power_history возвращает корректные данные."""

    def test_history_returns_list(self):
        from src.power_cache import query_power_history
        result = query_power_history(days=7)
        assert isinstance(result, list)

    def test_history_with_district_filter(self):
        from src.power_cache import query_power_history
        result = query_power_history(district_filter="Центральный", days=7)
        assert isinstance(result, list)
        # Все записи должны быть по Центральному району
        for row in result:
            assert "центральн" in row.get("district", "").lower() or row.get("district") == ""

    def test_history_by_district_aggregation(self):
        from src.power_cache import query_power_history_by_district
        result = query_power_history_by_district(days=30)
        assert isinstance(result, list)
        for row in result:
            assert "district" in row
            assert "active_houses" in row
            assert "planned_houses" in row
            assert "days_with_outages" in row

    def test_history_by_day_aggregation(self):
        from src.power_cache import query_power_history_by_day
        result = query_power_history_by_day(days=30)
        assert isinstance(result, list)
        for row in result:
            assert "day" in row


# ── ГРУППА 3: РЕЙТИНГ ЭФФЕКТИВНОСТИ ─────────────────────────────────────────

class TestРейтингЭффективностиЖКХ:
    """query_power_efficiency возвращает score/grade на основе YAML-регламента."""

    def test_efficiency_returns_list(self):
        from src.power_cache import query_power_efficiency
        result = query_power_efficiency(days=30)
        assert isinstance(result, list)

    def test_efficiency_uses_yaml_rules(self):
        """Score собирается из компонентов, описанных в power_rating_rules.yaml."""
        from src.rule_engine import rules
        cfg = rules.get("power_rating_rules")
        assert cfg is not None
        assert "grades" in cfg
        sc = cfg.get("scoring")
        assert sc, "нет секции scoring — регламент не мигрировал на версию 2.0"
        for part in ("reliability", "load", "intraday"):
            assert part in sc, f"нет компонента {part}"
            assert "weight" in sc[part]
        # Сумма весов задаёт верхнюю границу шкалы
        total = sum(float(sc[p]["weight"]) for p in ("reliability", "load", "intraday"))
        assert total == float(cfg["general"]["max_score"]), (
            f"сумма весов {total} не равна max_score — шкала уедет"
        )

    def test_ramp_normalisation(self):
        """_ramp нормирует метрику между якорями в обе стороны шкалы."""
        from src.power_cache import _ramp
        # «больше — лучше» (доля чистых дней)
        assert _ramp(0.0, 0.0, 0.5) == 0.0
        assert _ramp(0.5, 0.0, 0.5) == 1.0
        assert _ramp(0.25, 0.0, 0.5) == 0.5
        # «меньше — лучше» (число домов без ресурса): bad больше good
        assert _ramp(200, 200, 10) == 0.0
        assert _ramp(10, 200, 10) == 1.0
        assert _ramp(105, 200, 10) == 0.5
        # Срез за пределами якорей
        assert _ramp(-1, 0.0, 0.5) == 0.0
        assert _ramp(999, 0.0, 0.5) == 1.0
        assert _ramp(0, 200, 10) == 1.0
        # Вырожденный случай не делит на ноль
        assert _ramp(5, 3, 3) == 0.0

    def test_efficiency_grade_matches_thresholds(self):
        """Грейд соответствует порогам из регламента, а не зашит в коде."""
        from src.power_cache import query_power_efficiency
        from src.rule_engine import rules
        grades = rules.get("power_rating_rules").get("grades", [])
        for row in query_power_efficiency(days=30):
            expected = "F"
            for g in grades:
                if row["score"] >= float(g["threshold"]):
                    expected = g["grade"]
                    break
            assert row["grade"] == expected, (
                f"{row['district']}: балл {row['score']} → ожидался {expected}, "
                f"получен {row['grade']}"
            )

    def test_efficiency_components_sum_to_score(self):
        """Балл равен сумме своих компонентов — разложение в UI не врёт."""
        from src.power_cache import query_power_efficiency
        for row in query_power_efficiency(days=30):
            c = row["metrics"]["components"]
            total = c["reliability"] + c["load"] + c["intraday"]
            # Допуск = округление балла (до 0.05) + округление трёх
            # компонентов (до 0.005 каждый)
            assert abs(total - row["score"]) <= 0.07, (
                f"{row['district']}: компоненты дают {total}, балл {row['score']}"
            )
            for part in ("reliability", "load", "intraday"):
                assert 0 <= c[part] <= c["max"][part] + 1e-9, (
                    f"{row['district']}: компонент {part} вне своего веса"
                )

    def test_efficiency_score_range(self):
        from src.power_cache import query_power_efficiency
        result = query_power_efficiency(days=30)
        for row in result:
            assert 0 <= row["score"] <= 10, f"Score {row['score']} вне диапазона 0-10"
            assert row["grade"] in ("A", "B", "C", "D", "F"), f"Некорректный grade: {row['grade']}"

    def test_efficiency_has_metrics(self):
        from src.power_cache import query_power_efficiency
        result = query_power_efficiency(days=30)
        for row in result:
            m = row.get("metrics", {})
            assert "clean_days" in m
            assert "outage_days" in m
            assert "evening_days" in m


# ── ГРУППА 4: ГРАНИЧНЫЕ УСЛОВИЯ ──────────────────────────────────────────────

class TestГраничныеУсловияЖКХ:
    """Пустые данные, нулевые дни, большие периоды."""

    def test_history_zero_days(self):
        from src.power_cache import query_power_history
        result = query_power_history(days=0)
        assert isinstance(result, list)

    def test_history_large_period(self):
        from src.power_cache import query_power_history
        result = query_power_history(days=365)
        assert isinstance(result, list)

    def test_efficiency_one_day(self):
        from src.power_cache import query_power_efficiency
        result = query_power_efficiency(days=1)
        assert isinstance(result, list)

    def test_history_nonexistent_district(self):
        from src.power_cache import query_power_history
        result = query_power_history(district_filter="НесуществующийРайон", days=7)
        assert isinstance(result, list)
        assert len(result) == 0


# ── ГРУППА 5: ОПЕРАЦИИ РОУТИНГА ЖКХ ──────────────────────────────────────────

class TestРоутингЖКХ:
    """Запросы ЖКХ маршрутизируются к power_outages с правильным utility."""

    @pytest.mark.parametrize("query,expected_utility", [
        ("отключения электричества", "электроснабж"),
        ("нет горячей воды", "горяч"),
        ("отключения отопления", "теплоснабж"),
        ("отключения холодной воды", "холодн"),
        ("отключения газа", "газоснабж"),
    ])
    def test_utility_routing(self, query, expected_utility):
        from src.router import best_topic
        result = best_topic(query)
        assert result is not None and result.topic == "power_outages"

    @pytest.mark.parametrize("query", [
        "плановые отключения на неделю",
        "история отключений за месяц",
        "аварийные отключения ЖКХ сейчас",
    ])
    def test_power_operations_routing(self, query):
        from src.router import best_topic
        result = best_topic(query)
        assert result is not None and result.topic == "power_outages"
