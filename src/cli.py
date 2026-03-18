"""CLI NSK OpenData Bot.

Команды:
  bot topics            — список поддерживаемых тем
  bot ask "<запрос>"   — задать вопрос на русском
  bot update [--all | --topic <name>]  — обновить кэш
  bot serve             — запустить HTTP API (FastAPI)
"""

import logging
import sys

import click
from rich.console import Console

console = Console()

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(name)s: %(message)s",
)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Подробный вывод логов")
def cli(verbose: bool) -> None:
    """NSK OpenData Bot — агент для открытых данных Новосибирска."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


@cli.command()
def topics() -> None:
    """Показать поддерживаемые темы и статус кэша."""
    from .renderer import render_topics
    render_topics()


@cli.command()
@click.argument("query_text")
@click.option("--auto-update", is_flag=True, help="Автоматически обновить устаревший кэш")
@click.option("--limit", "-n", default=None, type=int, help="Лимит строк в выводе")
def ask(query_text: str, auto_update: bool, limit: int | None) -> None:
    """Задать вопрос на русском языке.

    Примеры:
      bot ask "сколько парковок в Центральном районе"
      bot ask "топ-5 остановок с павильонами"
      bot ask "покажи библиотеки в Ленинском районе"
      bot ask "сколько школ по районам"
    """
    from .router import route, best_topic
    from .planner import make_plan
    from .executor import execute_plan
    from .renderer import render_ask_result, render_no_topic
    from .fetcher import is_stale, load_meta
    from .registry import get_dataset
    from .cache import get_table_info, table_exists

    # 1. Маршрутизация
    route_result = best_topic(query_text)
    all_routes = route(query_text)

    if not route_result:
        from .planner import INFO_PATTERNS, DISTRICTS_PATTERNS
        from .renderer import render_help, render_districts
        q_lower = query_text.lower()
        if DISTRICTS_PATTERNS.search(q_lower):
            render_districts()
        elif INFO_PATTERNS.search(q_lower):
            render_help()
        else:
            render_no_topic(query_text, all_routes)
        sys.exit(0)

    topic = route_result.topic

    # ── Тема строительства — отдельная ветка ──
    if topic == "construction":
        from .construction_opendata import permits_available, get_construction_meta
        from .executor import execute_construction
        from .renderer import render_construction_result
        from .fetcher import is_stale

        needs_update = not permits_available() or is_stale("construction_permits", 24)
        if needs_update:
            if auto_update:
                console.print("[yellow]Обновляю данные о строительстве...[/yellow]")
                _do_update("construction_permits")
                _do_update("construction_commissioned")
            elif not permits_available():
                console.print(
                    "[yellow]⚠ Данные о строительстве не загружены.[/yellow]\n"
                    "  Запустите: [bold]bot construction update[/bold]\n"
                    f"  или:       [bold]bot ask --auto-update \"{query_text}\"[/bold]"
                )
                sys.exit(1)
            else:
                console.print(
                    "[dim]⚠ Кэш данных о строительстве устарел (> 24ч). "
                    "Обновить: bot construction update[/dim]"
                )

        plan = make_plan(query_text, topic)
        if limit:
            plan.limit = limit
        result = execute_construction(plan)
        meta = get_construction_meta()
        render_construction_result(query_text, plan, result, meta)
        return

    # ── Тема отключений электроснабжения — отдельная ветка ──
    if topic == "power_outages":
        from .power_scraper import fetch_all_outages
        from .power_cache import upsert_outages, is_power_stale, get_power_meta
        from .executor import execute_power
        from .renderer import render_power_result

        if is_power_stale():
            if auto_update:
                console.print("[yellow]Обновляю данные об отключениях...[/yellow]")
                records = fetch_all_outages()
                upsert_outages(records)
            else:
                console.print(
                    "[dim]⚠ Данные об отключениях могут быть устаревшими (>30 мин)."
                    " Обновить: bot power update[/dim]"
                )

        plan = make_plan(query_text, topic)
        if limit:
            plan.limit = limit
        result = execute_power(plan)
        meta = get_power_meta()
        render_power_result(query_text, plan, result, meta)
        return

    # ── Стандартные темы opendata ──
    ds = get_dataset(topic)

    # 2. Проверяем кэш
    if not table_exists(topic) or is_stale(topic, ds.get("ttl_hours", 24)):
        if auto_update:
            console.print(f"[yellow]Кэш устарел, обновляю данные для '{ds.get('name')}'...[/yellow]")
            _do_update(topic)
        elif not table_exists(topic):
            console.print(
                f"[yellow]⚠ Данные для темы '{ds.get('name')}' не загружены.[/yellow]\n"
                f"  Запустите: [bold]bot update --topic {topic}[/bold]\n"
                f"  или:       [bold]bot ask --auto-update \"{query_text}\"[/bold]"
            )
            sys.exit(1)
        else:
            console.print(
                f"[dim]⚠ Кэш устарел (> 24ч), используем последние данные. "
                f"Обновить: bot update --topic {topic}[/dim]"
            )

    # 3. Планирование
    plan = make_plan(query_text, topic)
    if limit:
        plan.limit = limit

    # 4. Выполнение
    result = execute_plan(plan)

    # 5. Рендер
    cache_info = load_meta().get(topic, {})
    cache_info.update(get_table_info(topic))
    render_ask_result(query_text, route_result, plan, result, cache_info)


@cli.command()
@click.option("--all", "update_all", is_flag=True, help="Обновить все темы")
@click.option("--topic", "topic_name", default=None, help="Обновить конкретную тему")
@click.option("--force", is_flag=True, help="Обновить даже если кэш актуален")
def update(update_all: bool, topic_name: str | None, force: bool) -> None:
    """Скачать/обновить данные из opendata.novo-sibirsk.ru.

    Примеры:
      bot update --all
      bot update --topic parking
      bot update --topic schools --force
    """
    from .registry import list_topics, get_dataset
    from .fetcher import is_stale

    if not update_all and not topic_name:
        console.print("[yellow]Укажите --all или --topic <name>[/yellow]")
        console.print("Доступные темы: " + ", ".join(list_topics()))
        return

    topics_to_update = list_topics() if update_all else [topic_name]

    console.print(f"\n[bold]Обновление кэша NSK OpenData Bot[/bold]")
    console.print(f"Тем к обновлению: {len(topics_to_update)}\n")

    ok = 0
    failed = 0
    skipped = 0

    for t in topics_to_update:
        ds = get_dataset(t)
        if not ds:
            console.print(f"[red]Неизвестная тема: {t}[/red]")
            failed += 1
            continue

        if not force and not is_stale(t, ds.get("ttl_hours", 24)):
            from .fetcher import load_meta
            m = load_meta().get(t, {})
            rows = m.get("rows", "?")
            console.print(f"  [dim]— {ds.get('name')} ({t}): кэш актуален ({rows} строк), пропускаем[/dim]")
            skipped += 1
            continue

        rows = _do_update(t)
        if rows > 0:
            ok += 1
        else:
            failed += 1

    console.print(f"\n[bold]Итог:[/bold] обновлено={ok}, пропущено={skipped}, ошибок={failed}")


def _do_update(topic: str) -> int:
    """Выполняет загрузку и кэширование одной темы. Возвращает кол-во строк."""
    from .registry import get_dataset
    from .fetcher import fetch_csv
    from .parser import read_csv
    from .cache import load_into_db
    from .renderer import render_update_start, render_update_done, render_update_error

    ds = get_dataset(topic)
    if not ds:
        return 0

    name = ds.get("name", topic)
    url = ds.get("data_url", "")

    render_update_start(topic, name, url)

    try:
        out_file = fetch_csv(topic, url, ttl_hours=ds.get("ttl_hours", 24), force=True)
        if not out_file:
            render_update_error(topic, "Не удалось скачать файл")
            return 0

        rows = read_csv(out_file, ds)
        if not rows:
            render_update_error(topic, "Пустой файл или ошибка парсинга")
            return 0

        count = load_into_db(topic, rows, ds)
        render_update_done(topic, count)
        return count

    except Exception as e:
        render_update_error(topic, str(e))
        return 0


@cli.group()
def power() -> None:
    """Данные об отключениях электроснабжения (051.novo-sibirsk.ru).

    Примеры:
      bot power update               — обновить данные прямо сейчас
      bot power status               — текущее состояние
      bot power status --district "Советский район"
      bot power planned              — плановые отключения
      bot power history              — история за 7 дней
      bot power history --days 3     — история за 3 дня
    """
    pass


@power.command(name="update")
@click.option("--force", is_flag=True, help="Обновить даже если данные актуальны")
def power_update(force: bool) -> None:
    """Скачать актуальные данные об отключениях с 051.novo-sibirsk.ru."""
    from .power_scraper import fetch_all_outages
    from .power_cache import upsert_outages, is_power_stale, get_power_meta

    if not force and not is_power_stale():
        meta = get_power_meta()
        last = meta.get("last_scraped", "")
        console.print(f"[dim]Данные актуальны (обновлены {last[:16]}). Используйте --force для принудительного обновления.[/dim]")
        return

    console.print("[cyan]↓[/cyan] Загружаю данные с 051.novo-sibirsk.ru...")
    records = fetch_all_outages()
    if not records:
        console.print("[red]✗ Нет данных — проверьте соединение с интернетом[/red]")
        return

    added = upsert_outages(records)
    meta = get_power_meta()
    console.print(f"[green]✓[/green] Получено: {added} записей")
    console.print(
        f"  Активных домов:   [red]{meta['active_houses']}[/red]\n"
        f"  Плановых домов:   [yellow]{meta['planned_houses']}[/yellow]\n"
        f"  Всего в архиве:   {meta['total_records']}"
    )


@power.command(name="status")
@click.option("--district", "-d", default=None, help="Фильтр по району")
@click.option("--all-utilities", is_flag=True, help="Показать все системы (не только электро)")
def power_status(district: str | None, all_utilities: bool) -> None:
    """Показать текущий статус отключений."""
    from .power_cache import query_power, get_power_meta, is_power_stale
    from .power_scraper import fetch_all_outages
    from .power_cache import upsert_outages

    if is_power_stale():
        console.print("[yellow]Данные устарели, обновляю...[/yellow]")
        upsert_outages(fetch_all_outages())

    utility_filter = None if all_utilities else "электроснабж"
    rows = query_power(
        utility_filter=utility_filter,
        district_filter=district,
        latest_only=True,
    )
    meta = get_power_meta()

    from .planner import Plan
    from .renderer import render_power_result
    plan = Plan(
        operation="POWER_STATUS",
        topic="power_outages",
        district=district,
        street=None,
        limit=50,
        year=None,
        min_value=None,
    )
    result = {
        "operation": "POWER_STATUS",
        "rows": rows,
        "columns": ["utility", "group_type", "district", "houses", "scraped_at"],
        "count": len(rows),
    }
    render_power_result("Текущий статус отключений", plan, result, meta)


@power.command(name="planned")
@click.option("--district", "-d", default=None, help="Фильтр по району")
def power_planned(district: str | None) -> None:
    """Показать плановые отключения (из последнего обновления)."""
    from .power_cache import query_power, get_power_meta, is_power_stale
    from .power_scraper import fetch_all_outages
    from .power_cache import upsert_outages

    if is_power_stale():
        console.print("[yellow]Данные устарели, обновляю...[/yellow]")
        upsert_outages(fetch_all_outages())

    rows = query_power(
        utility_filter="электроснабж",
        district_filter=district,
        group_filter="planned",
        latest_only=True,
    )
    meta = get_power_meta()

    from .planner import Plan
    from .renderer import render_power_result
    plan = Plan(
        operation="POWER_PLANNED",
        topic="power_outages",
        district=district,
        street=None,
        limit=50,
        year=None,
        min_value=None,
    )
    result = {
        "operation": "POWER_PLANNED",
        "rows": rows,
        "columns": ["utility", "district", "houses", "scraped_at"],
        "count": len(rows),
    }
    render_power_result("Плановые отключения электроснабжения", plan, result, meta)


@power.command(name="history")
@click.option("--days", "-n", default=7, type=int, help="Глубина истории в днях (макс. 7)")
@click.option("--district", "-d", default=None, help="Фильтр по району")
def power_history(days: int, district: str | None) -> None:
    """Показать историю отключений за последние N дней."""
    from .power_cache import get_history_by_day, get_power_meta

    days = min(days, 7)
    rows = get_history_by_day(
        utility_filter="электроснабж",
        district_filter=district,
        days=days,
    )
    meta = get_power_meta()

    from .planner import Plan
    from .renderer import render_power_result
    plan = Plan(
        operation="POWER_HISTORY",
        topic="power_outages",
        district=district,
        street=None,
        limit=days,
        year=None,
        min_value=None,
    )
    result = {
        "operation": "POWER_HISTORY",
        "rows": rows,
        "columns": ["day", "group_type", "total_houses", "snapshots"],
        "count": len(rows),
    }
    render_power_result(f"История отключений за {days} дней", plan, result, meta)


@cli.group()
def ecology() -> None:
    """Качество воздуха и погода в Новосибирске (Open-Meteo + CityAir).

    Примеры:
      bot ecology update               — загрузить актуальные данные
      bot ecology status               — текущий AQI и погода по всем районам
      bot ecology status --district "Советский район"
      bot ecology pdk                  — превышение ПДК PM2.5 сегодня
      bot ecology history              — динамика за 7 дней
      bot ecology history --days 3 --district "Центральный район"
    """
    pass


@ecology.command(name="update")
@click.option("--force", is_flag=True, help="Обновить даже если данные актуальны")
def ecology_update(force: bool) -> None:
    """Загрузить актуальные данные о качестве воздуха и погоде."""
    from .ecology_fetcher import fetch_all_ecology
    from .ecology_cache import upsert_stations, upsert_measurements, is_ecology_stale, get_ecology_meta

    if not force and not is_ecology_stale():
        meta = get_ecology_meta()
        last = meta.get("last_updated", "")
        console.print(f"[dim]Данные актуальны (обновлены {last[:16]}). Используйте --force для принудительного обновления.[/dim]")
        return

    console.print("[cyan]↓[/cyan] Загружаю данные Open-Meteo (воздух + погода)...")
    upsert_stations()
    records = fetch_all_ecology()
    if not records:
        console.print("[red]✗ Нет данных — проверьте соединение с интернетом[/red]")
        return

    added = upsert_measurements(records)
    meta = get_ecology_meta()
    console.print(f"[green]✓[/green] Получено: {added} измерений по {meta['districts_covered']} районам")
    console.print(f"  Обновлено: {meta['last_updated'][:19]}")


@ecology.command(name="status")
@click.option("--district", "-d", default=None, help="Фильтр по району")
def ecology_status(district: str | None) -> None:
    """Показать текущее качество воздуха и погоду по районам."""
    from .ecology_cache import query_current, get_ecology_meta, is_ecology_stale
    from .ecology_fetcher import fetch_all_ecology
    from .ecology_cache import upsert_stations, upsert_measurements

    if is_ecology_stale():
        console.print("[yellow]Данные устарели, обновляю...[/yellow]")
        upsert_stations()
        upsert_measurements(fetch_all_ecology())

    rows = query_current(district_filter=district)
    meta = get_ecology_meta()

    if not rows:
        console.print("[yellow]Нет данных о качестве воздуха.[/yellow]")
        return

    from rich.table import Table
    tbl = Table(title=f"Качество воздуха и погода — {meta.get('last_updated', '')[:16]}")
    tbl.add_column("Район",          style="cyan", no_wrap=True)
    tbl.add_column("AQI",            justify="right")
    tbl.add_column("PM2.5",          justify="right")
    tbl.add_column("PM10",           justify="right")
    tbl.add_column("NO2",            justify="right")
    tbl.add_column("Темп °C",        justify="right")
    tbl.add_column("Ветер м/с",      justify="right")
    tbl.add_column("Влажн. %",       justify="right")

    for r in rows:
        aqi = r.get("aqi")
        aqi_str = str(aqi) if aqi is not None else "—"
        aqi_color = "green" if aqi and aqi < 50 else ("yellow" if aqi and aqi < 100 else "red")
        tbl.add_row(
            r.get("district", ""),
            f"[{aqi_color}]{aqi_str}[/{aqi_color}]",
            str(r.get("pm25") or "—"),
            str(r.get("pm10") or "—"),
            str(r.get("no2")  or "—"),
            str(r.get("temperature_c") or "—"),
            str(r.get("wind_speed_ms") or "—"),
            str(r.get("humidity_pct") or "—"),
        )
    console.print(tbl)
    console.print(f"[dim]Источник: Open-Meteo / CityAir | Обновлено: {meta.get('last_updated', '')[:19]}[/dim]")


@ecology.command(name="pdk")
@click.option("--district", "-d", default=None, help="Фильтр по району")
def ecology_pdk(district: str | None) -> None:
    """Показать превышения ПДК PM2.5 > 35 мкг/м³ за сегодня."""
    from .ecology_cache import query_pdk_exceedances, is_ecology_stale
    from .ecology_fetcher import fetch_all_ecology
    from .ecology_cache import upsert_stations, upsert_measurements

    if is_ecology_stale():
        console.print("[yellow]Данные устарели, обновляю...[/yellow]")
        upsert_stations()
        upsert_measurements(fetch_all_ecology())

    rows = query_pdk_exceedances(district_filter=district)
    if not rows:
        console.print("[green]✓ Превышений ПДК WHO по PM2.5 не зафиксировано за сегодня.[/green]")
        return

    from rich.table import Table
    tbl = Table(title="Превышение ПДК PM2.5 (порог ВОЗ: 35 мкг/м³)", style="red")
    tbl.add_column("Район",      style="cyan")
    tbl.add_column("PM2.5 макс", justify="right", style="red bold")
    tbl.add_column("PM2.5 ср",   justify="right")
    tbl.add_column("Измерений",  justify="right")
    tbl.add_column("Последнее",  style="dim")
    for r in rows:
        tbl.add_row(
            r.get("district", ""),
            str(r.get("pm25_max") or "—"),
            str(r.get("pm25_avg") or "—"),
            str(r.get("измерений") or "—"),
            str(r.get("последнее", ""))[:16],
        )
    console.print(tbl)


@ecology.command(name="history")
@click.option("--days", "-n", default=7, type=int, help="Глубина истории в днях")
@click.option("--district", "-d", default=None, help="Фильтр по району")
def ecology_history(days: int, district: str | None) -> None:
    """Показать динамику качества воздуха и погоды за N дней."""
    from .ecology_cache import query_history

    days = min(days, 7)
    rows = query_history(district_filter=district, days=days)
    if not rows:
        console.print("[yellow]Нет исторических данных. Запустите: bot ecology update[/yellow]")
        return

    from rich.table import Table
    tbl = Table(title=f"Динамика за {days} дней" + (f" — {district}" if district else ""))
    tbl.add_column("День",        style="cyan")
    tbl.add_column("Район",       style="dim")
    tbl.add_column("PM2.5 ср",    justify="right")
    tbl.add_column("PM2.5 макс",  justify="right")
    tbl.add_column("AQI ср",      justify="right")
    tbl.add_column("Темп °C",     justify="right")
    tbl.add_column("Ветер м/с",   justify="right")
    for r in rows:
        tbl.add_row(
            str(r.get("день", "")),
            str(r.get("район", "")),
            str(r.get("pm25_ср") or "—"),
            str(r.get("pm25_макс") or "—"),
            str(r.get("aqi_ср") or "—"),
            str(r.get("темп_ср") or "—"),
            str(r.get("ветер_ср") or "—"),
        )
    console.print(tbl)


def _is_port_in_use(host: str, port: int) -> bool:
    """Возвращает True если порт уже занят."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex((host, port)) == 0


def _kill_on_port(port: int) -> bool:
    """Убивает процесс, слушающий порт. Возвращает True при успехе."""
    import subprocess
    try:
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True, text=True,
        )
        pids = [p.strip() for p in result.stdout.strip().splitlines() if p.strip()]
        if not pids:
            return False
        subprocess.run(["kill"] + pids, check=True)
        return True
    except Exception:
        return False


@cli.command()
@click.option("--host", default="127.0.0.1", help="Хост (по умолчанию 127.0.0.1)")
@click.option("--port", default=8000, type=int, help="Порт (по умолчанию 8000)")
@click.option("--restart", is_flag=True, help="Остановить уже запущенный сервер и стартовать заново")
def serve(host: str, port: int, restart: bool) -> None:
    """Запустить HTTP API (FastAPI) на http://<host>:<port>.

    Эндпоинты:
      GET  /topics          — список тем
      GET  /ask?q=<текст>   — запрос
      POST /update?topic=<name>  — обновить тему

    Если сервер уже запущен:
      bot serve --restart        — перезапустить
      bot stop                   — только остановить
    """
    if _is_port_in_use(host, port):
        if restart:
            console.print(f"[yellow]⏹  Останавливаю сервер на порту {port}...[/yellow]")
            if _kill_on_port(port):
                import time; time.sleep(0.8)   # ждём освобождения порта
                console.print("[green]✓ Сервер остановлен.[/green]\n")
            else:
                console.print(f"[red]✗ Не удалось остановить процесс. Попробуйте вручную:[/red]")
                console.print(f"  [bold]kill $(lsof -ti:{port})[/bold]")
                sys.exit(1)
        else:
            console.print(f"[red]✗ Порт {port} занят — сервер уже запущен.[/red]")
            console.print(f"  Открыть браузер:  [bold cyan]open http://{host}:{port}[/bold cyan]")
            console.print(f"  Остановить:       [bold]bot stop[/bold]")
            console.print(f"  Перезапустить:    [bold]bot serve --restart[/bold]")
            sys.exit(1)

    try:
        import uvicorn
        from .api import app
    except ImportError:
        console.print("[red]Для запуска сервера установите: pip install fastapi uvicorn[/red]")
        sys.exit(1)

    console.print(f"[bold green]Фреймворк Сигма API запущен: http://{host}:{port}[/bold green]")
    console.print("[dim]Ctrl+C — остановить  ·  bot serve --restart — перезапустить[/dim]")
    uvicorn.run(app, host=host, port=port, log_level="warning")


@cli.command()
@click.option("--port", default=8000, type=int, help="Порт сервера (по умолчанию 8000)")
def stop(port: int) -> None:
    """Остановить запущенный сервер.

    Примеры:
      bot stop
      bot stop --port 8080
    """
    if not _is_port_in_use("127.0.0.1", port):
        console.print(f"[dim]Сервер на порту {port} не запущен.[/dim]")
        return

    if _kill_on_port(port):
        console.print(f"[green]✓ Сервер на порту {port} остановлен.[/green]")
    else:
        console.print(f"[red]✗ Не удалось остановить автоматически. Выполните вручную:[/red]")
        console.print(f"  [bold]kill $(lsof -ti:{port})[/bold]")


@cli.group()
def construction() -> None:
    """Данные о строительстве в Новосибирске (opendata.novo-sibirsk.ru).

    Примеры:
      bot construction update          — загрузить/обновить данные (датасеты 124 и 125)
      bot construction status          — сводка: сколько разрешений, ввод, активных строек
      bot construction active          — список активных строек
      bot construction active --district "Калининский район"
    """
    pass


@construction.command(name="update")
@click.option("--force", is_flag=True, help="Обновить даже если данные актуальны")
def construction_update(force: bool) -> None:
    """Загрузить/обновить данные о строительстве с opendata.novo-sibirsk.ru."""
    from .fetcher import is_stale

    for topic_key in ("construction_permits", "construction_commissioned"):
        if not force and not is_stale(topic_key, 24):
            from .fetcher import load_meta
            m = load_meta().get(topic_key, {})
            rows_n = m.get("rows", "?")
            console.print(f"[dim]— {topic_key}: кэш актуален ({rows_n} строк), пропускаем[/dim]")
            continue
        _do_update(topic_key)

    from .construction_opendata import get_construction_meta
    meta = get_construction_meta()
    console.print(
        f"\n[bold]Итог:[/bold]"
        f" разрешений={meta['permits_total']}"
        f" | введено={meta['commissioned_total']}"
        f" | активных строек~{meta['active_total']}"
    )


@construction.command(name="status")
def construction_status() -> None:
    """Показать сводку по строительству."""
    from .construction_opendata import get_construction_meta, permits_available

    if not permits_available():
        console.print("[yellow]Данные не загружены. Запустите: bot construction update[/yellow]")
        return

    meta = get_construction_meta()
    console.print("\n[bold]Строительство в Новосибирске[/bold]")
    console.print(f"  Разрешения на строительство:  [cyan]{meta['permits_total']}[/cyan]  (обновлено {meta.get('permits_updated', '')[:10] or '—'})")
    console.print(f"  Введено в эксплуатацию:       [green]{meta['commissioned_total']}[/green]  (обновлено {meta.get('commissioned_updated', '')[:10] or '—'})")
    console.print(f"  Активных строек (разность):   [yellow]{meta['active_total']}[/yellow]")
    console.print("\n[dim]Источник: opendata.novo-sibirsk.ru (датасеты 124 и 125)[/dim]")


@construction.command(name="active")
@click.option("--district", "-d", default=None, help="Фильтр по району")
@click.option("--limit", "-n", default=20, type=int, help="Максимум строк")
def construction_active(district: str | None, limit: int) -> None:
    """Показать список активных строек (разрешение выдано, ввод в эксплуатацию не оформлен)."""
    from .construction_opendata import query_active, get_construction_meta, permits_available

    if not permits_available():
        console.print("[yellow]Данные не загружены. Запустите: bot construction update[/yellow]")
        return

    rows, total = query_active(district_filter=district, limit=limit)
    meta = get_construction_meta()

    from .planner import Plan
    from .renderer import render_construction_result
    plan = Plan(
        operation="CONSTRUCTION_ACTIVE",
        topic="construction",
        district=district,
        street=None,
        limit=limit,
        year=None,
        min_value=None,
    )
    result = {
        "operation": "CONSTRUCTION_ACTIVE",
        "rows": rows,
        "columns": ["NomRazr", "DatRazr", "Zastr", "NameOb", "AdrOr", "district", "KadNom"],
        "count": total,
        "shown": len(rows),
        "limit": limit,
        "note": "Активные стройки = разрешения без ввода в эксплуатацию",
    }
    render_construction_result(
        f"Активные стройки{f' в {district}' if district else ''}",
        plan, result, meta,
    )


if __name__ == "__main__":
    cli()
