"""Загрузка реестра наборов данных из config/datasets.yaml."""

from pathlib import Path
from typing import Any
import yaml

_CONFIG_PATH = Path(__file__).parent.parent / "config" / "datasets.yaml"
_registry: dict[str, Any] | None = None


def load_registry() -> dict[str, Any]:
    """Загружает и возвращает полный реестр датасетов."""
    global _registry
    if _registry is None:
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        defaults = raw.get("defaults", {})
        datasets = raw.get("datasets", {})
        # Применяем defaults к каждому датасету
        for name, ds in datasets.items():
            for key, val in defaults.items():
                if key not in ds:
                    ds[key] = val
            ds["id"] = name
        _registry = datasets
    return _registry


def get_dataset(topic: str) -> dict[str, Any] | None:
    """Возвращает конфиг датасета по имени темы."""
    return load_registry().get(topic)


def list_topics() -> list[str]:
    """Список всех поддерживаемых тем."""
    return list(load_registry().keys())
