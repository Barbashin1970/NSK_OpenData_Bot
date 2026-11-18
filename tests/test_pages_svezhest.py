"""
Свежесть демо Pages для планового запуска (docs/PAGES-DEMO-RAZBOR.md, Р-20).

18.11: демо не обновлялось больше часа — токена GITHUB_PAGES_TOKEN на Railway нет, ежечасного
запуска с сервера не было ни разу, а расписание GitHub за 22 часа сработало дважды. Запасной ход
усилен: слотов расписания три в час, а лишний запуск выходит сразу, если опубликованному
снимку меньше 45 минут.
"""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

KOREN = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("pages_svezhest", KOREN / "scripts" / "pages_svezhest.py")
ps = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ps)

SEJCHAS = datetime(2026, 11, 18, 5, 0, tzinfo=timezone.utc)


def test_возраст_снимка_в_минутах():
    zaprosy = []
    def poluchit(url):
        zaprosy.append(url)
        return b'{"snapshot": "2026-11-18T04:30:00+00:00", "saved": 840}'
    assert ps.vozrast_min("https://x.github.io/R/", SEJCHAS, poluchit) == 30.0
    assert zaprosy[0].startswith("https://x.github.io/R/api/_snapshot.json?t=")   # мимо кэша Pages


def test_снимок_не_прочитан_значит_собираем(capsys):
    def slomano(url):
        raise OSError("404")
    assert ps.vozrast_min("https://x.github.io/R", SEJCHAS, slomano) is None
    assert ps.vyvod(None, 45) == ["vozrast_min=", "svezhij=0"]
    assert ps.vyvod(30.0, 45) == ["vozrast_min=30", "svezhij=1"]
    assert ps.vyvod(50.0, 45) == ["vozrast_min=50", "svezhij=0"]


def test_workflow_три_слота_и_пропуск_свежего():
    wf = (KOREN / ".github" / "workflows" / "pages.yml").read_text(encoding="utf-8")
    assert 'cron: "7,27,47 * * * *"' in wf
    assert "scripts/pages_svezhest.py" in wf and "if: github.event_name == 'schedule'" in wf
    # сборка, выгрузка и публикация пропускаются только при свежем снимке; пуш и запуск
    # с Railway собирают всегда (шаг свежести у них не выполняется — выход пустой)
    assert wf.count("if: steps.svezhest.outputs.svezhij != '1'") == 3
