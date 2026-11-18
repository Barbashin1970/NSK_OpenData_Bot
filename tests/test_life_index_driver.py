"""
══════════════════════════════════════════════════════════════════════════════
  ИНДЕКС «ВОДИТЕЛЬ»: мороз и ветер, гололёд — отдельной ветвью по влаге
  src/routes/ecology.py: _compute_life_indices · src/static/index.html: computeLifeIndices
══════════════════════════════════════════════════════════════════════════════

Находка сессии РАГРАФ 18.11: индекс давал +5 при средней температуре от −3 до +2 °C без
всякого условия на осадки — прежний «сухой ноль — гололёд», от которого оповещения отказались
17.11 (правило не прошло проверку на ДТП, отчёт РАГРАФ-ПРОГНОЗ.НИР.001-2026, п. 9.2). Теперь —
как в РАГРАФ: индекс считает мороз и ветер, гололёд — по уровню ice_level на сегодня
(«гололёд» +5, «возможна гололедица» +3).
"""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

KOREN = Path(__file__).resolve().parent.parent


def stroki(t, w=3.0):
    return [{"temperature_c": t, "wind_speed_ms": w, "pm25": 5.0, "aqi": 20}]


class TestСервер:
    def test_сухо_около_нуля_без_штрафа(self):
        from src.routes.ecology import _compute_life_indices
        assert _compute_life_indices(stroki(0.5))["driver"]["score"] == 0

    @pytest.mark.parametrize("uroven, ballov", [("glaze", 5), ("refreeze", 3), (None, 0)])
    def test_гололёд_по_влаге(self, uroven, ballov):
        from src.routes.ecology import _compute_life_indices
        assert _compute_life_indices(stroki(0.5), ice_level=uroven)["driver"]["score"] == ballov

    def test_мороз_и_ветер_как_прежде(self):
        from src.routes.ecology import _compute_life_indices
        assert _compute_life_indices(stroki(-22.0))["driver"]["score"] == 2          # сильный мороз
        assert _compute_life_indices(stroki(-5.0, w=13.0))["driver"]["score"] == 2   # только ветер: лёд по температуре больше не начисляется


def funkciya_tablo() -> str:
    """Текст computeLifeIndices из страницы — по балансу фигурных скобок."""
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    i = html.index("function computeLifeIndices(")
    j, glubina = html.index("{", i), 0
    for k in range(j, len(html)):
        glubina += {"{": 1, "}": -1}.get(html[k], 0)
        if glubina == 0:
            return html[i:k + 1]
    raise AssertionError("конец функции не найден")


@pytest.mark.parametrize("t, uroven, ballov", [(0.5, None, 0), (0.5, "glaze", 5), (0.5, "refreeze", 3),
                                               (-5.0, None, 0), (-22.0, None, 2)])
def test_табло_та_же_логика(tmp_path, t, uroven, ballov):
    node = shutil.which("node")
    if not node:
        pytest.skip("node не установлен")
    stend = (funkciya_tablo() + "\nconsole.log(JSON.stringify(computeLifeIndices("
             + json.dumps(stroki(t)) + ", 0, " + json.dumps(uroven) + ").driver.score));")
    f = tmp_path / "ix.js"
    f.write_text(stend, encoding="utf-8")
    assert json.loads(subprocess.run([node, str(f)], capture_output=True, text=True, check=True).stdout) == ballov


def test_табло_передаёт_уровень_гололёда():
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    assert "computeLifeIndices(ecoRows, eco?.snow_impact)" not in html          # все вызовы с ice_level
    assert len(re.findall(r"computeLifeIndices\([^)]*ice_level\)", html)) == 3


def test_регламент_индексов():
    yaml = pytest.importorskip("yaml")
    d = yaml.safe_load((KOREN / "config/rules/life_indices_rules.yaml").read_text(encoding="utf-8"))
    dr = d["driver"]
    assert d["version"] == "1.1" and dr["ice_glaze_score"] == 5 and dr["ice_refreeze_score"] == 3
    assert not any(k.startswith(("temp_black_ice", "temp_regular_ice")) for k in dr)
