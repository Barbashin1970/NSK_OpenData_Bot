"""
Сторож: встроенные скрипты index.html синтаксически целы.

Табло мэра и панель служащего — один большой встроенный скрипт. Одна пропущенная
скобка — и пустеет вся страница, а ни один Python-тест этого не заметит. Проверка
гоняет `node --check` по каждому встроенному <script>; без Node пропускается.
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

INDEX = Path(__file__).parent.parent / "src" / "static" / "index.html"


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js не установлен")
def test_встроенные_скрипты_index_html_разбираются(tmp_path):
    html = INDEX.read_text(encoding="utf-8")
    blocks = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", html, flags=re.S)
    assert blocks, "в index.html не найдено встроенных скриптов"
    for i, body in enumerate(blocks):
        f = tmp_path / f"inline_{i}.js"
        f.write_text(body, encoding="utf-8")
        r = subprocess.run(["node", "--check", str(f)], capture_output=True, text=True)
        assert r.returncode == 0, f"скрипт №{i} в index.html не разбирается:\n{r.stderr[:800]}"


def test_блок_прогноза_подключён_к_обоим_табло():
    html = INDEX.read_text(encoding="utf-8")
    assert html.count("${_renderZhkhForecast(forecast)}") == 2   # мэр и служащий
    assert "function _showForecastHelp()" in html and "_FC_QUESTIONS" in html
