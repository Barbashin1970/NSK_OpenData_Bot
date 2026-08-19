"""Движок регламентов: загрузка и кэширование YAML-правил из config/rules/.

Использование:
    from .rule_engine import rules

    r = rules.get("traffic_rules")
    weekend_scale = r["weekend_scale"]           # 0.45
    scooter_tip   = rules.transport_hint(today)  # сезонная подсказка
    is_scooter    = rules.is_scooter_season(today)

Горячая перезагрузка без рестарта сервера:
    rules.reload()  # перечитает все YAML-файлы с диска
"""

from __future__ import annotations

import logging
import shutil
from datetime import date
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)

# Директория с YAML-регламентами: Volume-путь (data/) приоритетнее config/
_RULES_DIR_VOLUME = Path(__file__).parent.parent / "data" / "rules"
_RULES_DIR_SEED = Path(__file__).parent.parent / "config" / "rules"
_RULES_DIR = _RULES_DIR_VOLUME if _RULES_DIR_VOLUME.parent.exists() else _RULES_DIR_SEED

# Известные файлы — список для документации; загружаются лениво по имени
_KNOWN_RULES = (
    "traffic_rules",
    "holiday_calendar",
    "ecology_rules",
    "life_indices_rules",
    "mobile_index_rules",
)


class RuleEngine:
    """Загрузчик YAML-регламентов с in-memory кэшем и поддержкой reload."""

    def __init__(self, rules_dir: Path = _RULES_DIR) -> None:
        self._dir   = rules_dir
        self._cache: dict[str, dict] = {}
        self._synced = False

    # ── Синхронизация с seed'ом по версии ─────────────────────────────────────

    @staticmethod
    def _ver_tuple(value: Any) -> tuple:
        """'2.0' → (2, 0). Нечисловые части игнорируются."""
        parts = []
        for chunk in str(value or "0").split("."):
            digits = "".join(c for c in chunk if c.isdigit())
            parts.append(int(digits) if digits else 0)
        return tuple(parts) or (0,)

    def sync_from_seed(self) -> list[str]:
        """Обновляет регламенты на Volume, если в поставке версия новее.

        Регламенты живут в data/rules/ (Volume), чтобы правки из Студии
        переживали редеплой. Из-за этого новая версия правил, приехавшая с
        кодом в config/rules/, сама на прод не попадала: файл на Volume уже
        существовал и молча побеждал. Схема расчёта при этом могла смениться —
        и балл считался по новым весам, а грейды брались по старым порогам.

        Сравниваем поле version: seed новее → копируем, сохраняя прежний файл
        рядом как .bak. Равные версии не трогаем — там могут быть правки,
        сделанные в Студии.
        """
        if self._dir == _RULES_DIR_SEED or not _RULES_DIR_SEED.exists():
            return []
        updated = []
        self._dir.mkdir(parents=True, exist_ok=True)
        for seed_path in sorted(_RULES_DIR_SEED.glob("*.yaml")):
            vol_path = self._dir / seed_path.name
            try:
                if not vol_path.exists():
                    shutil.copy2(seed_path, vol_path)
                    updated.append(seed_path.stem)
                    continue
                with open(seed_path, encoding="utf-8") as fh:
                    seed_ver = self._ver_tuple((yaml.safe_load(fh) or {}).get("version"))
                with open(vol_path, encoding="utf-8") as fh:
                    vol_ver = self._ver_tuple((yaml.safe_load(fh) or {}).get("version"))
                if seed_ver > vol_ver:
                    backup = vol_path.with_suffix(
                        f".v{'.'.join(str(p) for p in vol_ver)}.bak"
                    )
                    shutil.copy2(vol_path, backup)
                    shutil.copy2(seed_path, vol_path)
                    updated.append(seed_path.stem)
                    log.info(
                        "rule_engine: %s обновлён %s → %s (старый сохранён в %s)",
                        seed_path.stem, vol_ver, seed_ver, backup.name,
                    )
            except Exception as exc:
                log.error("rule_engine.sync_from_seed: %s — %s", seed_path.name, exc)
        if updated:
            self._cache.clear()
        self._synced = True
        return updated

    # ── Основной API ──────────────────────────────────────────────────────────

    def get(self, name: str) -> dict[str, Any]:
        """Вернуть регламент по имени файла (без .yaml).

        Результат кэшируется до следующего reload().
        Если файл не найден — возвращает пустой dict и пишет warning.
        """
        if name not in self._cache:
            self._cache[name] = self._load(name)
        return self._cache[name]

    def reload(self) -> list[str]:
        """Сбросить кэш и перезагрузить все ранее загруженные регламенты.

        Возвращает список успешно перезагруженных имён.
        """
        names = list(self._cache.keys())
        self._cache.clear()
        loaded = []
        for name in names:
            try:
                self._cache[name] = self._load(name)
                loaded.append(name)
            except Exception as exc:
                log.error("rule_engine.reload: ошибка загрузки '%s': %s", name, exc)
        log.info("rule_engine.reload: перезагружено %d регламентов: %s", len(loaded), loaded)
        return loaded

    def status(self) -> dict[str, Any]:
        """Состояние кэша — используется эндпоинтом /admin/rules-status."""
        result = {}
        for name in _KNOWN_RULES:
            path = self._dir / f"{name}.yaml"
            result[name] = {
                "cached":  name in self._cache,
                "file":    str(path),
                "exists":  path.exists(),
                "version": self._cache.get(name, {}).get("version"),
            }
        return result

    # ── Удобные хелперы ───────────────────────────────────────────────────────

    def is_scooter_season(self, dt: date | None = None) -> bool:
        """True, если сегодня (или дата dt) попадает в сезон самокатов."""
        if dt is None:
            dt = date.today()
        try:
            cfg = self.get("traffic_rules")
            scooter = cfg.get("seasonal", {}).get("scooter", {})
            from_str = scooter.get("available_from", "05-01")  # MM-DD
            to_str   = scooter.get("available_to",   "10-31")
            from_md = tuple(int(x) for x in from_str.split("-"))  # (MM, DD)
            to_md   = tuple(int(x) for x in to_str.split("-"))
            current = (dt.month, dt.day)
            return from_md <= current <= to_md
        except Exception:
            return False

    def transport_hint(self, dt: date | None = None) -> str:
        """Вернуть сезонную подсказку по транспорту для текста рекомендаций."""
        try:
            cfg     = self.get("traffic_rules")
            scooter = cfg.get("seasonal", {}).get("scooter", {})
            if self.is_scooter_season(dt):
                return scooter.get("hint_in_season", "метро, электросамокаты или пешие маршруты")
            return scooter.get("hint_out_season", "метро или пешие маршруты")
        except Exception:
            return "метро или пешие маршруты"

    def tip(self, level_key: str, role: str = "citizen", dt: date | None = None) -> str:
        """Вернуть текст рекомендации для уровня пробок.

        level_key: free | moderate | difficult | complex | very_complex | collapse
        role:      citizen | official
        {transport_hint} в тексте заменяется на сезонную подсказку.
        """
        try:
            cfg  = self.get("traffic_rules")
            text = cfg.get("tips", {}).get(level_key, {}).get(role, "")
            if "{transport_hint}" in text:
                text = text.replace("{transport_hint}", self.transport_hint(dt))
            return text
        except Exception:
            return ""

    # ── Внутренние методы ─────────────────────────────────────────────────────

    def _load(self, name: str) -> dict[str, Any]:
        path = self._dir / f"{name}.yaml"
        # Fallback: если нет в data/rules/, читаем из config/rules/
        if not path.exists() and self._dir != _RULES_DIR_SEED:
            path = _RULES_DIR_SEED / f"{name}.yaml"
        if not path.exists():
            log.warning("rule_engine: файл '%s' не найден, используем пустой dict", path)
            return {}
        with path.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        log.debug("rule_engine: загружен '%s' (version=%s)", name, data.get("version"))
        return data


# ── Глобальный singleton ──────────────────────────────────────────────────────
rules = RuleEngine()
