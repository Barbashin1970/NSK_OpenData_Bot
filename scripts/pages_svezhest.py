"""Свежесть демо-копии на Pages: сколько минут назад снят опубликованный снимок (Р-20).

Плановый запуск сборки (schedule, три слота в час) сначала спрашивает возраст опубликованного
снимка: моложе порога — сборка не нужна, Railway зря не нагружается. Основной ход — запуск
раз в час с Railway (src/pages_dispatch.py, нужен GITHUB_PAGES_TOKEN); расписание GitHub —
запасной: 17–18.11.2026 оно за 22 часа сработало дважды.

    python3 scripts/pages_svezhest.py <адрес_сайта> [порог_минут]  >> "$GITHUB_OUTPUT"

Печатает vozrast_min=<минуты> и svezhij=1|0. Снимок не прочитан — svezhij=0: лучше лишняя
сборка, чем устаревшее демо.
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone


def vozrast_min(base_url: str, sejchas: datetime | None = None, poluchit=None) -> float | None:
    """Возраст опубликованного снимка в минутах; None — снимок не прочитан."""
    url = base_url.rstrip("/") + f"/api/_snapshot.json?t={int(time.time())}"   # мимо кэша Pages
    poluchit = poluchit or (lambda u: urllib.request.urlopen(u, timeout=30).read())
    try:
        snimok = datetime.fromisoformat(json.loads(poluchit(url))["snapshot"])
    except Exception:
        return None
    return ((sejchas or datetime.now(timezone.utc)) - snimok).total_seconds() / 60


def vyvod(vozrast: float | None, porog: float) -> list[str]:
    svezhij = vozrast is not None and vozrast < porog
    return [f"vozrast_min={'' if vozrast is None else round(vozrast)}", f"svezhij={'1' if svezhij else '0'}"]


def main(argv: list[str]) -> None:
    if len(argv) < 2:
        sys.exit(__doc__)
    porog = float(argv[2]) if len(argv) > 2 else 45.0
    v = vozrast_min(argv[1])
    stroki = vyvod(v, porog)
    print("\n".join(stroki))
    print(f"снимку {'?' if v is None else round(v)} мин, порог {porog:g} — "
          f"{'сборка не нужна' if stroki[1].endswith('1') else 'собираем'}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv)
